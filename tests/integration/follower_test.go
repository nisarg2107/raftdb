package integration

import "testing"

func TestSingleFollowerCrashRecovery(t *testing.T) {
	g := start(t, 5)
	leader := g.leader()
	var follower *member
	for _, m := range g.members {
		if m != leader {
			follower = m
			break
		}
	}
	put(t, leader, "before-follower-crash", "safe")
	g.converged("before-follower-crash", "safe")
	g.stop(follower)
	put(t, leader, "while-follower-offline", "committed")
	g.open(follower)
	g.converged("while-follower-offline", "committed")
	g.converged("before-follower-crash", "safe")
}
