// Package client discovers the leader using configured addresses. Connections
// are reused; only explicit follower rejections can retry mutating requests.
package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"raftdb/api"
)

type Client struct {
	Addresses []string
	mu        sync.Mutex
	conns     map[string]*grpc.ClientConn
}

func New(addresses string) *Client {
	return &Client{Addresses: strings.Split(addresses, ","), conns: make(map[string]*grpc.ClientConn)}
}
func (c *Client) At(address string) (api.DatabaseClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn := c.conns[address]
	if conn == nil {
		var err error
		conn, err = grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.conns[address] = conn
	}
	return api.NewDatabaseClient(conn), nil
}
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
}

// Leader only trusts an address mapping present in the configured seed list.
// This also lets host-side clients discover through Docker's published ports.
func (c *Client) Leader(ctx context.Context) (api.DatabaseClient, string, error) {
	var last error
	// The first pass also starts lazy gRPC connections. A second bounded pass
	// allows resolver/dial setup to finish without an unbounded retry loop.
	for pass := 0; pass < 2; pass++ {
		for _, addr := range c.Addresses {
			if err := ctx.Err(); err != nil {
				return nil, "", err
			}
			rpc, err := c.At(addr)
			if err != nil {
				last = err
				continue
			}
			probe, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			s, err := rpc.Status(probe, &api.Empty{})
			cancel()
			if err != nil {
				last = err
				continue
			}
			if s.State == "Leader" {
				return rpc, addr, nil
			}
		}
	}
	if last == nil {
		last = errors.New("no leader among configured nodes")
	}
	return nil, "", last
}
func (c *Client) Do(ctx context.Context, operation func(api.DatabaseClient) error) error {
	for attempt := 0; attempt < 2; attempt++ {
		rpc, _, err := c.Leader(ctx)
		if err != nil {
			return err
		}
		err = operation(rpc)
		if status.Code(err) != codes.FailedPrecondition {
			return err
		}
	}
	return status.Error(codes.Unavailable, "leadership changed during request")
}
