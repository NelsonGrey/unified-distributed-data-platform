package replication

import (
	"testing"
	"time"
)

// TestAddNodeToRunningCluster is the correctness check for the control
// plane's first slice: a cluster started with one node should let an
// operator add more nodes live — the previous only way to change
// membership was restarting every process with a different --raft-peers,
// which isn't an operator workflow at all.
func TestAddNodeToRunningCluster(t *testing.T) {
	// Start a single-node cluster (bootstrap with just itself).
	single := newCluster(t, 1)
	leader := waitForLeader(t, single)

	if _, err := leader.node.Propose(Command{Op: OpPut, Key: []byte("before"), Value: []byte("v")}); err != nil {
		t.Fatalf("propose before scale-out: %v", err)
	}

	// Start two more raft nodes that are NOT part of the configuration yet
	// — they exist as processes but the cluster doesn't know about them.
	joiningPeers := []Peer{{ID: "n2", Addr: freeAddr(t)}, {ID: "n3", Addr: freeAddr(t)}}
	var joining []testNode
	for _, p := range joiningPeers {
		eng, err := openTestEngine(t)
		if err != nil {
			t.Fatalf("open engine for %s: %v", p.ID, err)
		}
		node, err := Open(eng, Config{ID: p.ID, BindAddr: p.Addr, DataDir: t.TempDir(), ApplyTimeout: 5 * time.Second})
		if err != nil {
			t.Fatalf("open joining node %s: %v", p.ID, err)
		}
		t.Cleanup(func() { node.Shutdown() })
		joining = append(joining, testNode{node: node, engine: eng})
	}

	for _, p := range joiningPeers {
		if err := leader.node.AddVoter(p.ID, p.Addr); err != nil {
			t.Fatalf("add voter %s: %v", p.ID, err)
		}
	}

	// The new nodes must catch up on the write that happened before they
	// joined (full log replay, not just future writes) and be reachable
	// via subsequent writes too.
	if _, err := leader.node.Propose(Command{Op: OpPut, Key: []byte("after"), Value: []byte("v")}); err != nil {
		t.Fatalf("propose after scale-out: %v", err)
	}

	all := append([]testNode{*leader}, joining...)
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok := true
		for _, n := range all {
			if _, _, found := n.engine.Get([]byte("before")); !found {
				ok = false
			}
			if _, _, found := n.engine.Get([]byte("after")); !found {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("joined nodes did not catch up (pre- and post-join writes) within timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}

	servers, err := leader.node.ListServers()
	if err != nil {
		t.Fatalf("list servers: %v", err)
	}
	if len(servers.Members) != 3 {
		t.Fatalf("expected 3 members after adding 2 nodes to a 1-node cluster, got %d: %+v", len(servers.Members), servers.Members)
	}
	for _, m := range servers.Members {
		if !m.IsVoter {
			t.Fatalf("expected every member to be a voter, got %+v", m)
		}
	}
}

func TestRemoveNodeFromRunningCluster(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := waitForLeader(t, nodes)

	servers, err := leader.node.ListServers()
	if err != nil {
		t.Fatalf("list servers: %v", err)
	}
	var victimID string
	for _, m := range servers.Members {
		if m.ID != servers.LeaderID {
			victimID = m.ID
			break
		}
	}
	if victimID == "" {
		t.Fatal("no non-leader member found to remove")
	}

	if err := leader.node.RemoveServer(victimID); err != nil {
		t.Fatalf("remove server: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		servers, err := leader.node.ListServers()
		if err != nil {
			t.Fatalf("list servers: %v", err)
		}
		if len(servers.Members) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected 2 members after removal, still have %d", len(servers.Members))
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The remaining 2-node cluster (still a majority of the original 3)
	// must continue to accept writes.
	if _, err := leader.node.Propose(Command{Op: OpPut, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatalf("propose after removal: %v", err)
	}
}
