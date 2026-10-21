package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"raftdb/api"
	"raftdb/internal/client"
)

// requireRefusal contacts each live RPC endpoint directly, so discovery failure
// cannot masquerade as proof that a minority server rejected consensus requests.
func requireRefusal(ctx context.Context, c *client.Client) error {
	for _, address := range c.Addresses {
		rpc, err := c.At(address)
		if err != nil {
			return err
		}
		probe, cancel := context.WithTimeout(ctx, 4*time.Second)
		health, err := rpc.Health(probe, &api.Empty{})
		cancel()
		if err != nil || !health.GetServing() {
			return fmt.Errorf("minority endpoint is not alive: %s: %v", address, err)
		}
		probe, cancel = context.WithTimeout(ctx, 4*time.Second)
		_, writeErr := rpc.Put(probe, &api.PutRequest{Key: "__minority_probe__", Value: "must-not-be-acknowledged"})
		cancel()
		probe, cancel = context.WithTimeout(ctx, 4*time.Second)
		_, readErr := rpc.Get(probe, &api.Key{Key: "__raftdb_readiness__"})
		cancel()
		probe, cancel = context.WithTimeout(ctx, 4*time.Second)
		_, batchWriteErr := rpc.BatchPut(probe, &api.BatchPutRequest{Entries: []*api.PutRequest{{Key: "__minority_batch__", Value: "must-not-be-acknowledged"}}})
		cancel()
		probe, cancel = context.WithTimeout(ctx, 4*time.Second)
		_, batchReadErr := rpc.BatchGet(probe, &api.BatchGetRequest{Keys: []string{"__raftdb_readiness__"}})
		cancel()
		for _, failure := range []error{writeErr, readErr, batchWriteErr, batchReadErr} {
			switch status.Code(failure) {
			case codes.FailedPrecondition, codes.Unavailable, codes.DeadlineExceeded:
			default:
				return fmt.Errorf("invalid minority result at %s: %v", address, failure)
			}
		}
		fmt.Printf("%s alive; unary write/read rejected (%s/%s); batch write/read rejected (%s/%s)\n", address, status.Code(writeErr), status.Code(readErr), status.Code(batchWriteErr), status.Code(batchReadErr))
	}
	return nil
}
