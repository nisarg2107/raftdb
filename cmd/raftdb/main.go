package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"raftdb/api"
	"raftdb/internal/cluster"
	"raftdb/internal/config"
	"raftdb/internal/metrics"
	"raftdb/internal/server"
)

func main() {
	if err := run(); err != nil {
		var flags *config.FlagError
		if errors.As(err, &flags) {
			os.Exit(2)
		}
		slog.Error("node failed", "error", err)
		os.Exit(1)
	}
}
func run() error {
	c, profile, err := config.Parse(os.Args[1:], os.Getenv)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(c.LogLevel)); err != nil {
		return err
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	n, err := cluster.Open(c)
	if err != nil {
		return err
	}
	defer n.Close()
	m := metrics.New(n)
	listener, err := net.Listen("tcp", c.GRPCAddress)
	if err != nil {
		return err
	}
	rpc := grpc.NewServer(grpc.UnaryInterceptor(server.Instrument(m)), grpc.MaxRecvMsgSize(512*1024))
	api.RegisterDatabaseServer(rpc, &server.Server{Node: n})
	reflection.Register(rpc)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	httpServer := &http.Server{Addr: c.MetricsAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errors := make(chan error, 3)
	go func() { errors <- rpc.Serve(listener) }()
	go func() { errors <- httpServer.ListenAndServe() }()
	if profile != "" {
		go func() { errors <- serveProfile(profile) }()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("node started", "id", c.ID, "grpc", c.GRPCAddress, "raft", c.RaftAddress)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	previous := ""
	var serveErr error
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case serveErr = <-errors:
			break loop
		case <-ticker.C:
			id, _ := n.Leader()
			state := n.Raft.State().String()
			term := n.Raft.Stats()["term"]
			current := id + state + term
			if current != previous {
				slog.Info("raft state changed", "leader", id, "state", state, "term", term)
				previous = current
			}
		}
	}
	done := make(chan struct{})
	go func() { rpc.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		rpc.Stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdown)
	slog.Info("node stopped", "id", c.ID)
	return serveErr
}
