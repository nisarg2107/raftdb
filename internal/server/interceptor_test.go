package server_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"raftdb/api"
	"raftdb/internal/metrics"
	"raftdb/internal/server"
)

// Keep the real recorder compatible; the test below checks both measurements.
var _ server.Recorder = (*metrics.Metrics)(nil)

type recordingMetrics struct {
	rpcCalls, operationCalls   int
	rpcMethod, operationMethod string
	rpcCode, operationCode     codes.Code
	operations                 int
}

func (m *recordingMetrics) Observe(method string, code codes.Code, _ time.Duration) {
	m.rpcCalls++
	m.rpcMethod, m.rpcCode = method, code
}

func (m *recordingMetrics) ObserveOperations(method string, code codes.Code, count int) {
	m.operationCalls++
	m.operationMethod, m.operationCode, m.operations = method, code, count
}

func TestInstrumentationPreservesRPCAndLogicalOperationMeasurements(t *testing.T) {
	cases := []struct {
		name    string
		request any
		count   int
	}{
		{"Get", &api.Key{Key: "a"}, 1},
		{"Put", &api.PutRequest{Key: "a", Value: "one"}, 1},
		{"BatchGet", &api.BatchGetRequest{Keys: []string{"a", "b"}}, 2},
		{"BatchPut", &api.BatchPutRequest{Entries: []*api.PutRequest{{Key: "a"}, {Key: "b"}}}, 2},
		{"Health", &api.Empty{}, 0},
	}
	for _, test := range cases {
		for _, code := range []codes.Code{codes.OK, codes.InvalidArgument, codes.DeadlineExceeded} {
			t.Run(test.name+"/"+code.String(), func(t *testing.T) {
				observations := &recordingMetrics{}
				interceptor := server.Instrument(observations)
				method := "/raftdb.v1.Database/" + test.name
				wantResponse := &api.Empty{}
				wantError := status.Error(code, "example result")
				response, err := interceptor(context.Background(), test.request,
					&grpc.UnaryServerInfo{FullMethod: method},
					func(_ context.Context, request any) (any, error) {
						if request != test.request {
							t.Fatal("handler received a different request")
						}
						return wantResponse, wantError
					})
				if response != wantResponse || err != wantError {
					t.Fatal("instrumentation changed the handler result")
				}
				if observations.rpcCalls != 1 || observations.operationCalls != 1 ||
					observations.rpcMethod != method || observations.operationMethod != method ||
					observations.rpcCode != code || observations.operationCode != code ||
					observations.operations != test.count {
					t.Fatalf("incorrect measurements: %+v", observations)
				}
			})
		}
	}
}
