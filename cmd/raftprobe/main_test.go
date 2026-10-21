package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raftdb/api"
)

type healthServer struct {
	api.UnimplementedDatabaseServer
}

func (*healthServer) Health(context.Context, *api.Empty) (*api.HealthResponse, error) {
	return &api.HealthResponse{Serving: true}, nil
}
func TestFailureObservationBracketsRPCShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	api.RegisterDatabaseServer(server, &healthServer{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rpc := api.NewDatabaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	initial := time.Now()
	if _, err := rpc.Health(ctx, &api.Empty{}); err != nil {
		t.Fatal(err)
	}
	observations := observeFailure(ctx, rpc, initial)
	time.Sleep(20 * time.Millisecond)
	beforeStop := time.Now()
	server.Stop()
	afterStop := time.Now()
	observation := <-observations
	if observation.err != nil {
		t.Fatal(observation.err)
	}
	if observation.failedAt.Before(beforeStop) || observation.lastHealthyStart.After(afterStop) || observation.failedAt.Before(observation.lastHealthyStart) {
		t.Fatal("invalid failure bracket", observation)
	}
}
func TestRecoveryQuantiles(t *testing.T) {
	median, p95 := quantiles([]float64{6, 2, 5, 1, 4, 3})
	if median != 3.5 || p95 != 6 {
		t.Fatal(median, p95)
	}
	median, p95 = quantiles([]float64{.9})
	if median != .9 || p95 != .9 {
		t.Fatal(median, p95)
	}
}
