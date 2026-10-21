package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"raftdb/internal/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "raftctl:", err)
		os.Exit(1)
	}
}

func run() error {
	addresses := flag.String("addresses", "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053,127.0.0.1:50054,127.0.0.1:50055", "comma-separated reachable gRPC addresses")
	timeout := flag.Duration("timeout", 5*time.Second, "total request deadline")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		return errors.New("usage: raftctl [flags] put KEY VALUE|get KEY|batch-get KEY...|batch-put KEY VALUE...|check-prefix COUNT VALUE|delete KEY|local KEY|status|health|snapshot|leader|seed [COUNT]")
	}
	c := client.New(*addresses)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	return execute(ctx, c, args)
}
