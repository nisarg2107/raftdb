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
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	addresses := flag.String("addresses", "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053,127.0.0.1:50054,127.0.0.1:50055", "reachable gRPC addresses")
	mode := flag.String("mode", "ready", "ready|validate|recovery|refuse")
	count := flag.Int("count", 10000, "expected seeded keys")
	trials := flag.Int("trials", 5, "recovery trials")
	project := flag.String("project", "raftdb-verify", "Compose project")
	flag.Parse()
	c := client.New(*addresses)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if *mode == "refuse" {
		return requireRefusal(ctx, c)
	}
	if err := ready(ctx, c); err != nil {
		return err
	}
	switch *mode {
	case "ready":
		fmt.Println("five healthy voters and stable leader confirmed")
		return nil
	case "validate":
		return validate(ctx, c, *count)
	case "recovery":
		return recovery(c, *project, *trials)
	default:
		return errors.New("unknown probe mode")
	}
}
