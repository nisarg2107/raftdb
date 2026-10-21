package server

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"raftdb/api"
)

type Recorder interface {
	Observe(method string, code codes.Code, elapsed time.Duration)
}

func Instrument(recorder Recorder) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		response, err := handler(ctx, req)
		recorder.Observe(info.FullMethod, status.Code(err), time.Since(start))
		if operations, ok := recorder.(interface{ ObserveOperations(string, codes.Code, int) }); ok {
			count := 0
			switch r := req.(type) {
			case *api.BatchGetRequest:
				count = len(r.GetKeys())
			case *api.BatchPutRequest:
				count = len(r.GetEntries())
			case *api.PutRequest, *api.Key:
				count = 1
			}
			operations.ObserveOperations(info.FullMethod, status.Code(err), count)
		}
		if err != nil {
			slog.Debug("request failed", "method", info.FullMethod, "error", err)
		}
		return response, err
	}
}
