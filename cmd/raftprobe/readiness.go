package main

import (
	"context"
	"errors"
	"time"

	"raftdb/api"
	"raftdb/internal/client"
)

// ready requires all five voters, agreement, and a successful quorum read.
func ready(ctx context.Context, c *client.Client) error {
	if len(c.Addresses) != 5 {
		return errors.New("readiness requires five configured nodes")
	}
	lastLeader := ""
	stableSince := time.Now()
	for ctx.Err() == nil {
		leader := ""
		healthy := true
		for _, addr := range c.Addresses {
			rpc, err := c.At(addr)
			if err != nil {
				return err
			}
			probe, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			s, err := rpc.Status(probe, &api.Empty{})
			cancel()
			if err != nil || len(s.GetMembers()) != 5 || s.GetLeaderId() == "" {
				healthy = false
				break
			}
			for _, m := range s.Members {
				if m.Suffrage != "Voter" {
					healthy = false
				}
			}
			if leader == "" {
				leader = s.LeaderId
			} else if leader != s.LeaderId {
				healthy = false
			}
		}
		if healthy && leader == lastLeader && time.Since(stableSince) >= 2*time.Second {
			probe, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := c.Do(probe, func(rpc api.DatabaseClient) error {
				const key = "__raftdb_readiness__"
				if _, err := rpc.Put(probe, &api.PutRequest{Key: key, Value: "ready"}); err != nil {
					return err
				}
				value, err := rpc.Get(probe, &api.Key{Key: key})
				if err != nil {
					return err
				}
				if value.Value != "ready" {
					return errors.New("readiness read mismatch")
				}
				return nil
			})
			cancel()
			if err == nil {
				return nil
			}
			healthy = false
		}
		if !healthy || leader != lastLeader {
			lastLeader = leader
			stableSince = time.Now()
		}
		time.Sleep(100 * time.Millisecond)
	}
	return ctx.Err()
}
