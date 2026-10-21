package main

import (
	"context"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"raftdb/api"
)

// Health failures bracket the client-observed outage using the same monotonic
// clock as the successful commit; Docker CLI startup is outside that interval.
type failureObservation struct {
	lastHealthyStart, failedAt time.Time
	err                        error
}

func observeFailure(ctx context.Context, rpc api.DatabaseClient, initial time.Time) <-chan failureObservation {
	observations := make(chan failureObservation, 1)
	go func() {
		last := initial
		for ctx.Err() == nil {
			started := time.Now()
			probe, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			health, err := rpc.Health(probe, &api.Empty{})
			cancel()
			if err == nil && health.GetServing() {
				last = started
			} else if status.Code(err) == codes.Unavailable {
				observations <- failureObservation{last, time.Now(), nil}
				return
			}
			// A slow health request alone is not evidence that the node died.
			time.Sleep(5 * time.Millisecond)
		}
		observations <- failureObservation{err: ctx.Err()}
	}()
	return observations
}

func quantiles(values []float64) (float64, float64) {
	sort.Float64s(values)
	median := values[len(values)/2]
	if len(values)%2 == 0 {
		median = (median + values[len(values)/2-1]) / 2
	}
	p95 := int(float64(len(values))*.95+.999) - 1
	return median, values[p95]
}
