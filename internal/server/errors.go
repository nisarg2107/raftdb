package server

import (
	"context"
	"errors"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"raftdb/internal/store"
)

func (s *Server) failure(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	_, leader := s.Node.Leader()
	if leader != "" {
		_ = grpc.SetTrailer(ctx, metadata.Pairs("leader-grpc", leader))
	}
	switch {
	case errors.Is(err, store.ErrInvalidBatch):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrBatchResultTooLarge):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, raft.ErrEnqueueTimeout):
		return status.Error(codes.DeadlineExceeded, "operation timed out; write outcome may be unknown")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, raft.ErrNotLeader):
		return status.Error(codes.FailedPrecondition, "contact the leader: "+leader)
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}

func validKey(key string) error {
	if err := store.ValidateKey(key); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}
