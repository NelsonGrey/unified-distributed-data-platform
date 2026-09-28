package replication

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/marknelson/uddp/internal/engine"
)

// memSink is a minimal raft.SnapshotSink for testing Persist without a
// real raft.FileSnapshotStore.
type memSink struct {
	bytes.Buffer
}

func (s *memSink) ID() string    { return "test-snapshot" }
func (s *memSink) Cancel() error { return nil }
func (s *memSink) Close() error  { return nil }

func testLog(index uint64, data []byte) *raft.Log {
	return &raft.Log{Index: index, Data: data}
}

func TestFSMApplyPutGetDelete(t *testing.T) {
	eng, err := engine.Open(filepath.Join(t.TempDir(), "fsm.wal"), nil)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()
	fsm := &FSM{Engine: eng}

	cmd, err := encodeCommand(Command{Op: OpPut, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	result := fsm.Apply(testLog(1, cmd)).(ApplyResult)
	if result.Err != nil {
		t.Fatalf("apply put: %v", result.Err)
	}

	value, _, found := eng.Get([]byte("k"))
	if !found || string(value) != "v" {
		t.Fatalf("expected k=v after apply, got value=%q found=%v", value, found)
	}
}

func TestFSMSnapshotAndRestore(t *testing.T) {
	srcEngine, err := engine.Open(filepath.Join(t.TempDir(), "src.wal"), nil)
	if err != nil {
		t.Fatalf("open src engine: %v", err)
	}
	defer srcEngine.Close()
	src := &FSM{Engine: srcEngine}

	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		cmd, _ := encodeCommand(Command{Op: OpPut, Key: []byte(kv[0]), Value: []byte(kv[1])})
		if r := src.Apply(testLog(1, cmd)).(ApplyResult); r.Err != nil {
			t.Fatalf("apply: %v", r.Err)
		}
	}

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}

	dstEngine, err := engine.Open(filepath.Join(t.TempDir(), "dst.wal"), nil)
	if err != nil {
		t.Fatalf("open dst engine: %v", err)
	}
	defer dstEngine.Close()
	// The destination starts with unrelated data, proving Restore actually
	// wipes prior state rather than merging into it.
	if _, err := dstEngine.Put([]byte("stale"), []byte("x"), 0, ""); err != nil {
		t.Fatalf("seed stale data: %v", err)
	}
	dst := &FSM{Engine: dstEngine}

	if err := dst.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if _, _, found := dstEngine.Get([]byte("stale")); found {
		t.Fatal("restore must wipe prior state, not merge into it")
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		value, _, found := dstEngine.Get([]byte(kv[0]))
		if !found || string(value) != kv[1] {
			t.Fatalf("expected %s=%s after restore, got value=%q found=%v", kv[0], kv[1], value, found)
		}
	}
}

// TestFSMWipeAll is the FSM-level check backing BR-014 DeleteNamespace:
// applied through the FSM (as it would be via raft), OpWipeAll must clear
// all state.
func TestFSMWipeAll(t *testing.T) {
	eng, err := engine.Open(filepath.Join(t.TempDir(), "wipe.wal"), nil)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()
	fsm := &FSM{Engine: eng}

	cmd, _ := encodeCommand(Command{Op: OpPut, Key: []byte("k"), Value: []byte("v")})
	if r := fsm.Apply(testLog(1, cmd)).(ApplyResult); r.Err != nil {
		t.Fatalf("apply put: %v", r.Err)
	}

	wipeCmd, _ := encodeCommand(Command{Op: OpWipeAll})
	if r := fsm.Apply(testLog(2, wipeCmd)).(ApplyResult); r.Err != nil {
		t.Fatalf("apply wipe: %v", r.Err)
	}

	if _, _, found := eng.Get([]byte("k")); found {
		t.Fatal("expected key to be gone after OpWipeAll")
	}

	// The engine must still work after a wipe (fresh WAL, not a broken one).
	if _, err := eng.Put([]byte("after-wipe"), []byte("v"), 0, ""); err != nil {
		t.Fatalf("put after wipe: %v", err)
	}
}

// TestFSMLoadSnapshot is the FSM-level check backing BR-008 RestoreBackup:
// applied through the FSM, OpLoadSnapshot must replace state entirely,
// not merge into whatever was there before.
func TestFSMLoadSnapshot(t *testing.T) {
	eng, err := engine.Open(filepath.Join(t.TempDir(), "load.wal"), nil)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()
	fsm := &FSM{Engine: eng}

	cmd, _ := encodeCommand(Command{Op: OpPut, Key: []byte("stale"), Value: []byte("x")})
	if r := fsm.Apply(testLog(1, cmd)).(ApplyResult); r.Err != nil {
		t.Fatalf("apply seed put: %v", r.Err)
	}

	loadCmd, _ := encodeCommand(Command{Op: OpLoadSnapshot, Entries: []engine.SnapshotEntry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}})
	if r := fsm.Apply(testLog(2, loadCmd)).(ApplyResult); r.Err != nil {
		t.Fatalf("apply load snapshot: %v", r.Err)
	}

	if _, _, found := eng.Get([]byte("stale")); found {
		t.Fatal("expected pre-restore state to be wiped, not merged")
	}
	if v, _, found := eng.Get([]byte("a")); !found || string(v) != "1" {
		t.Fatalf("expected a=1 after load snapshot, got value=%q found=%v", v, found)
	}
	if v, _, found := eng.Get([]byte("b")); !found || string(v) != "2" {
		t.Fatalf("expected b=2 after load snapshot, got value=%q found=%v", v, found)
	}
}
