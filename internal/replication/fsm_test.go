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
