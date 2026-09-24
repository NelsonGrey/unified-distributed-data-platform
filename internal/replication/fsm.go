package replication

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/raft"

	"github.com/marknelson/uddp/internal/engine"
)

// ApplyResult is what FSM.Apply returns (via raft's ApplyFuture.Response())
// for every command — the same Outcome/error pair the engine itself would
// have returned to a direct, non-replicated caller.
type ApplyResult struct {
	Outcome engine.Outcome
	Err     error
	Term    uint64 // the raft term (epoch) this command was committed under
}

// FSM adapts *engine.Engine to raft.FSM. Every node in the cluster runs
// its own FSM over its own local engine; raft's only job is ensuring every
// node applies the same commands in the same order (TRD 4.3: "epoch and
// leader/fencing checks prevent stale owners from accepting writes" — that
// fencing is raft's term/leader mechanism, not something this package
// re-implements).
type FSM struct {
	Engine *engine.Engine
}

var _ raft.FSM = (*FSM)(nil)

func (f *FSM) Apply(log *raft.Log) any {
	cmd, err := decodeCommand(log.Data)
	if err != nil {
		return ApplyResult{Err: err, Term: log.Term}
	}

	var out engine.Outcome
	switch cmd.Op {
	case OpPut:
		out, err = f.Engine.ApplyPut(cmd.Key, cmd.Value, cmd.ExpiresAtUnixNano, cmd.IdempotencyKey)
	case OpDelete:
		out, err = f.Engine.Delete(cmd.Key, cmd.IdempotencyKey)
	case OpCompareAndSet:
		out, err = f.Engine.ApplyCompareAndSet(cmd.Key, cmd.Value, cmd.ExpectedVersion, cmd.ExpiresAtUnixNano, cmd.IdempotencyKey)
	default:
		err = fmt.Errorf("replication: unknown op %d", cmd.Op)
	}

	return ApplyResult{Outcome: out, Err: err, Term: log.Term}
}

// Snapshot captures the engine's current live state. See the package doc
// for why this path is not expected to be exercised in this increment
// (SnapshotThreshold/Interval are set high enough in Node's config that
// raft shouldn't trigger it under normal operation) — it's implemented
// correctly regardless, both because raft's FSM interface requires it and
// because "implemented but never exercised" is not the same guarantee as
// "known to work," so it's covered by fsm_test.go directly.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{entries: f.Engine.Snapshot()}, nil
}

// Restore replaces the engine's entire state with what's in the snapshot.
// It's destructive (Engine.Reset wipes the local WAL) — correct only
// because Restore's whole contract is "this replica's prior state is
// stale/irrelevant, adopt the leader's snapshot instead."
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var entries []engine.SnapshotEntry
	if err := gob.NewDecoder(rc).Decode(&entries); err != nil {
		return fmt.Errorf("replication: decode snapshot: %w", err)
	}

	if err := f.Engine.Reset(); err != nil {
		return fmt.Errorf("replication: reset engine before restore: %w", err)
	}
	for _, e := range entries {
		if _, err := f.Engine.ApplyPut(e.Key, e.Value, e.ExpiresAtUnixNano, ""); err != nil {
			return fmt.Errorf("replication: restore key %q: %w", e.Key, err)
		}
	}
	return nil
}

type fsmSnapshot struct {
	entries []engine.SnapshotEntry
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s.entries); err != nil {
		sink.Cancel()
		return fmt.Errorf("replication: encode snapshot: %w", err)
	}
	if _, err := sink.Write(buf.Bytes()); err != nil {
		sink.Cancel()
		return fmt.Errorf("replication: write snapshot: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// AsApplyResult extracts an ApplyResult from a raft ApplyFuture's
// Response(), returning an error if the future itself failed (e.g. this
// node wasn't leader) or if Response() wasn't the type FSM.Apply returns
// (which would be an internal bug, not a client-facing condition).
func AsApplyResult(future raft.ApplyFuture) (ApplyResult, error) {
	if err := future.Error(); err != nil {
		return ApplyResult{}, err
	}
	result, ok := future.Response().(ApplyResult)
	if !ok {
		return ApplyResult{}, errors.New("replication: unexpected apply response type")
	}
	return result, nil
}
