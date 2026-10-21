package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"raftdb/api"
	"raftdb/internal/cluster"
	"raftdb/internal/store"
)

func (s *Server) BatchGet(ctx context.Context, r *api.BatchGetRequest) (*api.BatchGetResponse, error) {
	results, err := s.Node.ReadMany(ctx, r.GetKeys())
	if err != nil {
		return nil, s.failure(ctx, err)
	}
	response := &api.BatchGetResponse{Results: make([]*api.LookupResult, len(results))}
	for i, result := range results {
		response.Results[i] = &api.LookupResult{Found: result.Found, Value: result.Value}
	}
	return response, nil
}

func (s *Server) BatchPut(ctx context.Context, r *api.BatchPutRequest) (*api.Empty, error) {
	entries := r.GetEntries()
	if len(entries) < 1 || len(entries) > store.MaxAPIBatchOperations {
		return nil, status.Error(codes.InvalidArgument, "expected 1..128 operations")
	}
	commands := make([]store.Command, len(entries))
	for i, entry := range entries {
		commands[i] = store.Command{Op: "put", Key: entry.GetKey(), Value: entry.GetValue()}
	}
	if err := s.Node.WriteMany(ctx, commands); err != nil {
		return nil, s.failure(ctx, err)
	}
	return &api.Empty{}, nil
}

func (s *Server) Delete(ctx context.Context, r *api.Key) (*api.Empty, error) {
	if err := validKey(r.GetKey()); err != nil {
		return nil, err
	}
	return &api.Empty{}, s.failure(ctx, s.Node.Write(ctx, store.Command{Op: "delete", Key: r.Key}))
}

func (s *Server) Get(ctx context.Context, r *api.Key) (*api.Value, error) {
	if err := validKey(r.GetKey()); err != nil {
		return nil, err
	}
	v, ok, err := s.Node.Read(ctx, r.Key)
	if err != nil {
		return nil, s.failure(ctx, err)
	}
	if !ok {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &api.Value{Value: v}, nil
}

func (s *Server) GetLocal(_ context.Context, r *api.Key) (*api.Value, error) {
	if err := validKey(r.GetKey()); err != nil {
		return nil, err
	}
	v, ok := s.Node.LocalRead(r.Key)
	if !ok {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &api.Value{Value: v}, nil
}

func (s *Server) Put(ctx context.Context, r *api.PutRequest) (*api.Empty, error) {
	c := store.Command{Op: "put", Key: r.GetKey(), Value: r.GetValue()}
	if err := c.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &api.Empty{}, s.failure(ctx, s.Node.Write(ctx, c))
}

type Server struct {
	api.UnimplementedDatabaseServer
	Node *cluster.Node
}

func (s *Server) Snapshot(ctx context.Context, _ *api.Empty) (*api.Empty, error) {
	return &api.Empty{}, s.failure(ctx, s.Node.Snapshot(ctx))
}

func (s *Server) Status(ctx context.Context, _ *api.Empty) (*api.NodeStatus, error) {
	info, err := s.Node.Status(ctx)
	if err != nil {
		return nil, s.failure(ctx, err)
	}
	result := &api.NodeStatus{Id: info.ID, State: info.State, Term: info.Term,
		LeaderId: info.LeaderID, LeaderGrpc: info.LeaderGRPC, FirstLogIndex: info.FirstLogIndex,
		LastLogIndex: info.LastLogIndex, AppliedIndex: info.AppliedIndex, CommitIndex: info.CommitIndex}
	for _, member := range info.Members {
		result.Members = append(result.Members, &api.Member{Id: string(member.ID), RaftAddress: string(member.Address), Suffrage: member.Suffrage.String()})
	}
	return result, nil
}

// Health is process liveness. It does not assert quorum or read readiness.
func (s *Server) Health(context.Context, *api.Empty) (*api.HealthResponse, error) {
	return &api.HealthResponse{Serving: s.Node.Serving()}, nil
}
