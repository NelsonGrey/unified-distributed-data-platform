package replication

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/marknelson/uddp/internal/engine"
)

// Peer is one member of the raft cluster's configuration.
type Peer struct {
	ID   string
	Addr string
}

// Node owns a raft.Raft instance replicating a single partition's engine.
type Node struct {
	raft         *raft.Raft
	fsm          *FSM
	logger       io.Writer
	applyTimeout time.Duration
}

// Config configures a new replicated node.
type Config struct {
	ID           string // this node's raft server ID
	BindAddr     string // address this node's raft transport listens on
	DataDir      string // holds the raft log/stable store and snapshots (separate from the engine's own WAL)
	Bootstrap    bool   // true on exactly one node when first forming a cluster
	Peers        []Peer // the full cluster configuration, required when Bootstrap is true
	LogOutput    io.Writer
	ApplyTimeout time.Duration

	// TLS is optional. Unset means the raft transport is plaintext TCP —
	// fine for local dev/testing (e.g. the cluster_test.go suite), not for
	// crossing a real network. See TLSConfig's doc for why this is mTLS,
	// not just server-side TLS.
	TLS *TLSConfig
}

// Open starts a raft node over engine, bootstrapping a new cluster if
// cfg.Bootstrap is set, or expecting to be joined/already-configured
// otherwise (see AddVoter for adding a peer to a running cluster).
func Open(eng *engine.Engine, cfg Config) (*Node, error) {
	if cfg.LogOutput == nil {
		cfg.LogOutput = os.Stderr
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("replication: create data dir: %w", err)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.ID)
	raftConfig.LogOutput = cfg.LogOutput
	// Deferred scope (see package doc): this build never compacts the raft
	// log, so a new/lagging follower always catches up via full log
	// replay, never FSM.Restore. Setting these very high, rather than
	// disabling snapshots outright (raft has no clean "never" switch),
	// keeps that true under any realistic single-partition write volume
	// for this slice.
	raftConfig.SnapshotThreshold = 1 << 30
	raftConfig.SnapshotInterval = 24 * time.Hour

	fsm := &FSM{Engine: eng}

	logStorePath := filepath.Join(cfg.DataDir, "raft-log.bolt")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("replication: open raft log store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, cfg.LogOutput)
	if err != nil {
		return nil, fmt.Errorf("replication: open snapshot store: %w", err)
	}

	var transport raft.Transport
	if cfg.TLS != nil {
		layer, err := newTLSStreamLayer(cfg.BindAddr, *cfg.TLS)
		if err != nil {
			return nil, err
		}
		transport = raft.NewNetworkTransport(layer, 3, 10*time.Second, cfg.LogOutput)
	} else {
		log.Println("replication: WARNING starting raft transport without TLS — inter-node traffic (writes, heartbeats, elections) is plaintext; only safe for local development")
		addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
		if err != nil {
			return nil, fmt.Errorf("replication: resolve %s: %w", cfg.BindAddr, err)
		}
		transport, err = raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, cfg.LogOutput)
		if err != nil {
			return nil, fmt.Errorf("replication: open transport: %w", err)
		}
	}

	r, err := raft.NewRaft(raftConfig, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("replication: start raft: %w", err)
	}

	if cfg.Bootstrap {
		if len(cfg.Peers) == 0 {
			return nil, errors.New("replication: Bootstrap requires at least one peer (this node)")
		}
		servers := make([]raft.Server, len(cfg.Peers))
		for i, p := range cfg.Peers {
			servers[i] = raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Addr)}
		}
		f := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := f.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("replication: bootstrap cluster: %w", err)
		}
	}

	if cfg.ApplyTimeout == 0 {
		cfg.ApplyTimeout = 5 * time.Second
	}

	return &Node{raft: r, fsm: fsm, logger: cfg.LogOutput, applyTimeout: cfg.ApplyTimeout}, nil
}

// Propose replicates cmd through raft and returns once a quorum has
// durably committed it and this node has applied it locally. It fails
// with an error wrapping raft.ErrNotLeader (check via errors.Is) if this
// node isn't the current leader — TR-004's "reject stale writers" is
// exactly this: raft itself refuses to let a non-leader (or a
// leader whose term has been superseded) commit anything.
func (n *Node) Propose(cmd Command) (ApplyResult, error) {
	b, err := encodeCommand(cmd)
	if err != nil {
		return ApplyResult{}, err
	}
	future := n.raft.Apply(b, n.applyTimeout)
	return AsApplyResult(future)
}

// IsLeader reports whether this node currently believes it is the raft
// leader. Racy by nature (leadership can change immediately after this
// returns) — Propose is the authoritative check, this is for surfacing
// status (e.g. health/metrics), not for gating a decision.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the current leader's raft transport address, if
// known, so a client hitting the wrong node can be told where to retry.
func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// AddVoter adds a new voting member to a running cluster. Call this
// against the current leader (Propose-style calls to a non-leader fail;
// so does this).
func (n *Node) AddVoter(id, addr string) error {
	f := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	return f.Error()
}

// Shutdown stops this node's raft participation. It does not close the
// underlying engine — callers own that lifecycle separately.
func (n *Node) Shutdown() error {
	return n.raft.Shutdown().Error()
}
