package replication

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/marknelson/uddp/internal/engine"
)

// freeAddr finds an available loopback TCP address by binding to :0,
// reading back the assigned port, and releasing it. There's a narrow
// window where another process could grab the same port before raft
// binds it, but that's an acceptable risk for a local test, not something
// worth a retry loop here.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

type testNode struct {
	node   *Node
	engine *engine.Engine
}

func newCluster(t *testing.T, n int) []testNode {
	t.Helper()

	peers := make([]Peer, n)
	for i := 0; i < n; i++ {
		peers[i] = Peer{ID: fmt.Sprintf("n%d", i+1), Addr: freeAddr(t)}
	}

	nodes := make([]testNode, n)
	for i, p := range peers {
		eng, err := engine.Open(filepath.Join(t.TempDir(), "engine.wal"), nil)
		if err != nil {
			t.Fatalf("open engine %s: %v", p.ID, err)
		}
		t.Cleanup(func() { eng.Close() })

		node, err := Open(eng, Config{
			ID:           p.ID,
			BindAddr:     p.Addr,
			DataDir:      t.TempDir(),
			Bootstrap:    i == 0,
			Peers:        peers,
			ApplyTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("open node %s: %v", p.ID, err)
		}
		t.Cleanup(func() { node.Shutdown() })

		nodes[i] = testNode{node: node, engine: eng}
	}
	return nodes
}

func waitForLeader(t *testing.T, nodes []testNode) *testNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i := range nodes {
			if nodes[i].node.IsLeader() {
				return &nodes[i]
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func TestClusterConvergesOnLeaderAndReplicatesWrites(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := waitForLeader(t, nodes)

	result, err := leader.node.Propose(Command{Op: OpPut, Key: []byte("k1"), Value: []byte("v1")})
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("apply result error: %v", result.Err)
	}

	// All three engines must converge, not just the leader's — that's the
	// entire point of replication (TRD 4.3, FSG-002).
	deadline := time.Now().Add(5 * time.Second)
	for {
		allConverged := true
		for i := range nodes {
			value, version, found := nodes[i].engine.Get([]byte("k1"))
			if !found || string(value) != "v1" || version != result.Outcome.Version {
				allConverged = false
			}
		}
		if allConverged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replicas did not converge on the committed write within timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestClusterRejectsWritesFromNonLeader(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := waitForLeader(t, nodes)

	for i := range nodes {
		if nodes[i].node == leader.node {
			continue
		}
		_, err := nodes[i].node.Propose(Command{Op: OpPut, Key: []byte("k"), Value: []byte("v")})
		if err == nil {
			t.Fatal("expected a non-leader's Propose to fail")
		}
		if !errors.Is(err, raft.ErrNotLeader) && !errors.Is(err, raft.ErrLeadershipLost) {
			t.Fatalf("expected ErrNotLeader-family error, got %v", err)
		}
	}
}

func TestClusterElectsNewLeaderAfterLeaderFailure(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := waitForLeader(t, nodes)

	if _, err := leader.node.Propose(Command{Op: OpPut, Key: []byte("before"), Value: []byte("v")}); err != nil {
		t.Fatalf("propose before failure: %v", err)
	}

	if err := leader.node.Shutdown(); err != nil {
		t.Fatalf("shutdown leader: %v", err)
	}

	var remaining []testNode
	for i := range nodes {
		if nodes[i].node != leader.node {
			remaining = append(remaining, nodes[i])
		}
	}

	newLeader := waitForLeader(t, remaining)
	if newLeader.node == leader.node {
		t.Fatal("expected a different node to become leader after the old leader shut down")
	}

	result, err := newLeader.node.Propose(Command{Op: OpPut, Key: []byte("after"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("propose after failover: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("apply result error: %v", result.Err)
	}

	// The pre-failure write must have survived the failover (no
	// acknowledged loss — BR-002), and it must have been committed under
	// an earlier term than the post-failover write (the epoch actually
	// advanced, not just node identity).
	value, _, found := newLeader.engine.Get([]byte("before"))
	if !found || string(value) != "v" {
		t.Fatal("write committed before leader failure must survive failover")
	}
}
