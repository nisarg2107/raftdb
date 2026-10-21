package config

import (
	"flag"
	"strconv"
	"time"
)

// FlagError distinguishes flag syntax errors (CLI exit 2) from configuration
// errors (CLI exit 1), while leaving help as flag.ErrHelp.
type FlagError struct{ Err error }

func (e *FlagError) Error() string { return e.Err.Error() }
func (e *FlagError) Unwrap() error { return e.Err }

// Parse resolves defaults, then environment, then explicit flags.
// A private FlagSet makes parsing repeatable in tests and embedded callers.
func Parse(args []string, getenv func(string) string) (Config, string, error) {
	flags := flag.NewFlagSet("raftdb", flag.ContinueOnError)
	c := Default()
	if getenv("RAFTDB_PROFILE") == "tuned" {
		c.Heartbeat = 350 * time.Millisecond
		c.Election = 350 * time.Millisecond
		c.Lease = 175 * time.Millisecond
	}
	flags.StringVar(&c.ID, "id", c.ID, "unique node ID")
	flags.StringVar(&c.DataDir, "data", c.DataDir, "persistent directory")
	flags.StringVar(&c.RaftAddress, "raft", c.RaftAddress, "advertised and listening Raft address")
	flags.StringVar(&c.GRPCAddress, "grpc", c.GRPCAddress, "gRPC listen address")
	flags.StringVar(&c.MetricsAddress, "metrics", c.MetricsAddress, "HTTP metrics address")
	if peers := getenv("RAFTDB_PEERS"); peers != "" {
		c.Peers = peers
	}
	flags.StringVar(&c.Peers, "peers", c.Peers, "static id=raft-address=grpc-address peers")
	if backend := getenv("RAFTDB_STORAGE"); backend != "" {
		c.Storage = backend
	}
	if raw := getenv("RAFTDB_WRITE_PIPELINE"); raw != "" {
		var err error
		c.WritePipeline, err = strconv.Atoi(raw)
		if err != nil {
			return c, "", err
		}
	}
	flags.IntVar(&c.WritePipeline, "write-pipeline", c.WritePipeline, "maximum unresolved write batches (1..32; zero selects 8)")
	flags.StringVar(&c.Storage, "storage", c.Storage, "durable Raft storage: bolt or wal; existing state is migrated on change")
	flags.BoolVar(&c.Bootstrap, "bootstrap", false, "bootstrap once if no durable state exists")
	if raw := getenv("RAFTDB_BATCH_APPLY"); raw != "" {
		var err error
		c.BatchApply, err = strconv.ParseBool(raw)
		if err != nil {
			return c, "", err
		}
	}
	flags.BoolVar(&c.BatchApply, "batch-apply", c.BatchApply, "buffer apply requests for HashiCorp Raft group commitment")
	if raw := getenv("RAFTDB_READ_BATCH_WINDOW"); raw != "" {
		var err error
		c.ReadBatchWindow, err = time.ParseDuration(raw)
		if err != nil {
			return c, "", err
		}
	}
	flags.DurationVar(&c.ReadBatchWindow, "read-batch-window", c.ReadBatchWindow, "collect reads before a fresh quorum check; zero removes collection delay")
	flags.DurationVar(&c.Heartbeat, "heartbeat", c.Heartbeat, "heartbeat timeout")
	flags.DurationVar(&c.Election, "election", c.Election, "election timeout")
	flags.DurationVar(&c.Lease, "lease", c.Lease, "leader lease")
	flags.DurationVar(&c.OperationTimeout, "timeout", c.OperationTimeout, "maximum RPC operation wait")
	flags.DurationVar(&c.SnapshotInterval, "snapshot-interval", c.SnapshotInterval, "snapshot check interval")
	flags.Uint64Var(&c.SnapshotThreshold, "snapshot-threshold", c.SnapshotThreshold, "entries before snapshot")
	flags.Uint64Var(&c.TrailingLogs, "trailing-logs", c.TrailingLogs, "logs retained after snapshot")
	flags.StringVar(&c.LogLevel, "log-level", c.LogLevel, "DEBUG INFO WARN ERROR")
	profile := flags.String("pprof", getenv("RAFTDB_PPROF"), "optional private profiling listen address (e.g. 127.0.0.1:6060)")
	if err := flags.Parse(args); err != nil {
		if err != flag.ErrHelp {
			return c, "", &FlagError{Err: err}
		}
		return c, "", err
	}
	if _, err := c.Validate(); err != nil {
		return c, "", err
	}
	return c, *profile, nil
}
