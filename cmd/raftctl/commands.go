package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"raftdb/api"
	"raftdb/internal/client"
)

func execute(ctx context.Context, c *client.Client, args []string) error {
	switch args[0] {
	case "check-prefix":
		return checkPrefix(ctx, c, args)
	case "status", "health":
		for _, addr := range c.Addresses {
			rpc, err := c.At(addr)
			if err != nil {
				return err
			}
			var value interface{}
			if args[0] == "status" {
				value, err = rpc.Status(ctx, &api.Empty{})
			} else {
				value, err = rpc.Health(ctx, &api.Empty{})
			}
			if err != nil {
				return fmt.Errorf("%s: %w", addr, err)
			}
			b, _ := json.Marshal(value)
			fmt.Println(string(b))
		}
		return nil
	case "leader":
		_, addr, err := c.Leader(ctx)
		if err == nil {
			fmt.Println(addr)
		}
		return err
	case "local":
		if len(args) != 2 {
			return errors.New("local KEY")
		}
		rpc, err := c.At(c.Addresses[0])
		if err != nil {
			return err
		}
		v, err := rpc.GetLocal(ctx, &api.Key{Key: args[1]})
		if err == nil {
			fmt.Println(v.Value)
		}
		return err
	case "seed":
		return seed(ctx, c, args)
	}
	return c.Do(ctx, func(rpc api.DatabaseClient) error {
		switch args[0] {
		case "batch-get":
			if len(args) < 2 {
				return errors.New("batch-get KEY...")
			}
			response, err := rpc.BatchGet(ctx, &api.BatchGetRequest{Keys: args[1:]})
			if err == nil {
				b, _ := json.Marshal(response)
				fmt.Println(string(b))
			}
			return err
		case "batch-put":
			if len(args) < 3 || len(args)%2 != 1 {
				return errors.New("batch-put KEY VALUE...")
			}
			entries := make([]*api.PutRequest, 0, (len(args)-1)/2)
			for i := 1; i < len(args); i += 2 {
				entries = append(entries, &api.PutRequest{Key: args[i], Value: args[i+1]})
			}
			_, err := rpc.BatchPut(ctx, &api.BatchPutRequest{Entries: entries})
			return err
		case "put":
			if len(args) != 3 {
				return errors.New("put KEY VALUE")
			}
			_, err := rpc.Put(ctx, &api.PutRequest{Key: args[1], Value: args[2]})
			return err
		case "get":
			if len(args) != 2 {
				return errors.New("get KEY")
			}
			v, err := rpc.Get(ctx, &api.Key{Key: args[1]})
			if err == nil {
				fmt.Println(v.Value)
			}
			return err
		case "delete":
			if len(args) != 2 {
				return errors.New("delete KEY")
			}
			_, err := rpc.Delete(ctx, &api.Key{Key: args[1]})
			return err
		case "snapshot":
			_, err := rpc.Snapshot(ctx, &api.Empty{})
			return err
		default:
			return errors.New("unknown command: " + args[0])
		}
	})
}
