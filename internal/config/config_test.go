package config

import "testing"

func TestPeers(t *testing.T) {
	for _, raw := range []string{"", "bad", "a=x=y", "a=localhost:1=localhost:2,a=localhost:3=localhost:4", "a=localhost:1=localhost:2,b=localhost:1=localhost:3"} {
		if _, err := ParsePeers(raw); err == nil {
			t.Fatal(raw)
		}
	}
	if p, err := ParsePeers(Default().Peers); err != nil || len(p) != 1 {
		t.Fatal(p, err)
	}
}
