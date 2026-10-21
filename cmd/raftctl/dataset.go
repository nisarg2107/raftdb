package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"raftdb/api"
	"raftdb/internal/client"
)

func checkPrefix(ctx context.Context, c *client.Client, args []string) error {

	if len(args) != 3 {
		return errors.New("check-prefix COUNT VALUE")
	}
	count, err := strconv.Atoi(args[1])
	if err != nil || count < 1 {
		return errors.New("invalid prefix count")
	}
	for _, addr := range c.Addresses {
		rpc, err := c.At(addr)
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			key := fmt.Sprint("bench-", i)
			v, err := rpc.GetLocal(ctx, &api.Key{Key: key})
			if err != nil {
				return err
			}
			if v.Value != args[2] {
				return fmt.Errorf("%s: unexpected value for %s", addr, key)
			}
		}
	}
	return nil
}

func seed(ctx context.Context, c *client.Client, args []string) error {

	count := 10000
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(args[1])
		if err != nil || count <= 0 {
			return errors.New("invalid seed count")
		}
	}
	return c.Do(ctx, func(rpc api.DatabaseClient) error {
		// Bounded parallel dataset setup; no timings or throughput claims.
		setup, stop := context.WithCancel(ctx)
		defer stop()
		results := make(chan error, 16)
		for worker := 0; worker < 16; worker++ {
			go func(worker int) {
				for i := worker; i < count; i += 16 {
					if _, err := rpc.Put(setup, &api.PutRequest{Key: fmt.Sprintf("bench-%d", i), Value: "existing-value"}); err != nil {
						results <- err
						return
					}
				}
				results <- nil
			}(worker)
		}
		var firstError error
		for worker := 0; worker < 16; worker++ {
			if err := <-results; err != nil && firstError == nil {
				firstError = err
				stop()
			}
		}
		if firstError != nil {
			return firstError
		}
		fmt.Println("seeded", count)
		return nil
	})
}
