package main

import (
	"context"
	"errors"
	"fmt"

	"raftdb/api"
	"raftdb/internal/client"
)

func validate(ctx context.Context, c *client.Client, count int) error {

	if count <= 0 {
		return errors.New("validation count must be positive")
	}
	results := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		go func(worker int) {
			for i := worker; i < count; i += 8 {
				key := fmt.Sprintf("bench-%d", i)
				for _, addr := range c.Addresses {
					rpc, err := c.At(addr)
					if err != nil {
						results <- err
						return
					}
					v, err := rpc.GetLocal(ctx, &api.Key{Key: key})
					if err != nil || v.Value != "existing-value" {
						results <- fmt.Errorf("dataset validation failed: %s %s: %v", addr, key, err)
						return
					}
				}
			}
			results <- nil
		}(worker)
	}
	var firstError error
	for worker := 0; worker < 8; worker++ {
		if err := <-results; err != nil && firstError == nil {
			firstError = err
		}
	}
	if firstError != nil {
		return firstError
	}

	fmt.Println("complete seed dataset verified on every node")
	return nil
}
