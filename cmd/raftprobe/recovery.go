package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"raftdb/api"
	"raftdb/internal/client"
)

func compose(project string, args ...string) *exec.Cmd {
	base := []string{"compose", "-p", project}
	return exec.Command("docker", append(base, args...)...)
}

func recovery(c *client.Client, project string, trials int) error {
	if trials < 1 {
		return errors.New("trials must be positive")
	}
	var times, uppers []float64
	for trial := 0; trial < trials; trial++ {
		err := func() (trialError error) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := ready(ctx, c); err != nil {
				return err
			}
			rpc, _, err := c.Leader(ctx)
			if err != nil {
				return err
			}
			old, err := rpc.Status(ctx, &api.Empty{})
			if err != nil {
				return err
			}
			service := "raftdb-" + old.Id
			container, err := compose(project, "ps", "-q", service).Output()
			if err != nil || strings.TrimSpace(string(container)) == "" {
				return fmt.Errorf("resolve leader container: %v", err)
			}
			key := fmt.Sprintf("recovery-%d-%d", time.Now().UnixNano(), trial)
			if _, err := rpc.Put(ctx, &api.PutRequest{Key: key, Value: "committed-before-failure"}); err != nil {
				return err
			}
			healthyStart := time.Now()
			if _, err := rpc.Health(ctx, &api.Empty{}); err != nil {
				return err
			}
			failed := observeFailure(ctx, rpc, healthyStart)
			type killOutcome struct {
				at     time.Time
				err    error
				output string
			}
			killResults := make(chan killOutcome, 1)
			invoked := time.Now()
			go func() {
				killCtx, stop := context.WithTimeout(ctx, 10*time.Second)
				defer stop()
				var output bytes.Buffer
				command := exec.CommandContext(killCtx, "docker", "kill", "--signal", "KILL", strings.TrimSpace(string(container)))
				command.Stdout = &output
				command.Stderr = &output
				err := command.Run()
				killResults <- killOutcome{time.Now(), err, output.String()}
			}()
			// Always restart a killed node, including on validation failure.
			defer func() {
				output, err := compose(project, "start", service).CombinedOutput()
				if err != nil && trialError == nil {
					trialError = fmt.Errorf("restart: %w: %s", err, output)
				}
			}()
			var observation failureObservation
			var outcome *killOutcome
			select {
			case observation = <-failed:
			case result := <-killResults:
				outcome = &result
				if result.err != nil {
					return fmt.Errorf("kill: %w: %s", result.err, result.output)
				}
				observation = <-failed
			}
			if observation.err != nil {
				return observation.err
			}
			var elected time.Time
			for ctx.Err() == nil {
				probe, stop := context.WithTimeout(ctx, 1500*time.Millisecond)
				next, _, err := c.Leader(probe)
				if err == nil {
					info, err := next.Status(probe, &api.Empty{})
					if err == nil && info.Id != old.Id {
						if elected.IsZero() {
							elected = time.Now()
						}
						_, err = next.Put(probe, &api.PutRequest{Key: key + "-after", Value: "committed"})
						if err == nil {
							committed := time.Now()
							before, readErr := next.Get(probe, &api.Key{Key: key})
							stop()
							if readErr != nil || before.Value != "committed-before-failure" {
								return errors.New("committed value lost")
							}
							if outcome == nil {
								result := <-killResults
								outcome = &result
							}
							if outcome.err != nil {
								return fmt.Errorf("kill: %w: %s", outcome.err, outcome.output)
							}
							elapsed := committed.Sub(observation.failedAt).Seconds()
							upper := committed.Sub(observation.lastHealthyStart).Seconds()
							if elapsed < 0 {
								return errors.New("failure observation followed commit; invalid trial")
							}
							times = append(times, elapsed)
							uppers = append(uppers, upper)
							return json.NewEncoder(os.Stdout).Encode(map[string]interface{}{
								"trial": trial + 1, "old_leader": old.Id, "new_leader": info.Id,
								"failure_to_commit_seconds": elapsed, "failure_to_commit_upper_seconds": upper,
								"failure_observation_bracket_seconds": observation.failedAt.Sub(observation.lastHealthyStart).Seconds(),
								"election_seconds":                    elected.Sub(observation.failedAt).Seconds(),
								"docker_kill_command_seconds":         outcome.at.Sub(invoked).Seconds(),
								"kill_invocation_to_commit_seconds":   committed.Sub(invoked).Seconds(),
								"failure_observed_at":                 observation.failedAt.UTC().Format(time.RFC3339Nano),
								"last_healthy_probe_started_at":       observation.lastHealthyStart.UTC().Format(time.RFC3339Nano),
								"write_committed_at":                  committed.UTC().Format(time.RFC3339Nano),
							})
						}
					}
				}
				stop()
				time.Sleep(20 * time.Millisecond)
			}
			return errors.New("write availability did not recover before deadline")
		}()
		if err != nil {
			return err
		}
	}
	median, p95 := quantiles(times)
	medianUpper, p95Upper := quantiles(uppers)
	return json.NewEncoder(os.Stdout).Encode(map[string]interface{}{
		"trials": trials, "median_seconds": median, "p95_seconds": p95,
		"median_upper_seconds": medianUpper, "p95_upper_seconds": p95Upper,
		"definition": "first observed leader RPC unavailability to successful new-leader commit; actual failure bracket also reported",
	})
}
