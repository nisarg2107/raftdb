package config

import (
	"flag"
	"testing"
)

func TestParsePrecedenceAndIndependentFlagSets(t *testing.T) {
	env := map[string]string{
		"RAFTDB_WRITE_PIPELINE": "2", "RAFTDB_STORAGE": "wal",
		"RAFTDB_READ_BATCH_WINDOW": "200us", "RAFTDB_PROFILE": "tuned",
		"RAFTDB_PPROF": "127.0.0.1:6060",
	}
	get := func(key string) string { return env[key] }
	c, profile, err := Parse([]string{"-write-pipeline=1", "-heartbeat=2s"}, get)
	if err != nil {
		t.Fatal(err)
	}
	if c.WritePipeline != 1 || c.Storage != "wal" || c.Heartbeat.Seconds() != 2 || c.Election.Milliseconds() != 350 || c.ReadBatchWindow.Microseconds() != 200 || profile != env["RAFTDB_PPROF"] {
		t.Fatalf("precedence changed: %+v, profile=%s", c, profile)
	}
	c, _, err = Parse(nil, func(string) string { return "" })
	if err != nil || c.WritePipeline != 8 || c.Heartbeat.Seconds() != 1 || c.Storage != "bolt" {
		t.Fatalf("flags leaked between parses: %+v: %v", c, err)
	}
}

func TestParseRejectsInvalidConfigurationWithoutOpeningStorage(t *testing.T) {
	for _, args := range [][]string{{"-write-pipeline=33"}, {"-timeout=0"}, {"-read-batch-window=-1s"}, {"-storage=invalid"}, {"-id=absent"}} {
		if _, _, err := Parse(args, func(string) string { return "" }); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	for key, value := range map[string]string{"RAFTDB_WRITE_PIPELINE": "invalid", "RAFTDB_READ_BATCH_WINDOW": "invalid", "RAFTDB_BATCH_APPLY": "invalid"} {
		if _, _, err := Parse(nil, func(k string) string {
			if k == key {
				return value
			}
			return ""
		}); err == nil {
			t.Fatalf("accepted %s", key)
		}
	}
	if _, _, err := Parse([]string{"-help"}, func(string) string { return "" }); err != flag.ErrHelp {
		t.Fatal(err)
	}
}

func TestPeerEnvironmentCanBeOverriddenByFlag(t *testing.T) {
	c, _, err := Parse([]string{"-peers=" + Default().Peers}, func(key string) string {
		if key == "RAFTDB_PEERS" {
			return "invalid"
		}
		return ""
	})
	if err != nil || c.Peers != Default().Peers {
		t.Fatal(c, err)
	}
}
