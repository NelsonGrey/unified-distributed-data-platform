package engine

import (
	"path/filepath"
	"testing"
)

func TestSnapshotAndResetRoundTrip(t *testing.T) {
	e, _ := openTest(t)

	if _, err := e.Put([]byte("k1"), []byte("v1"), 0, ""); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := e.Put([]byte("k2"), []byte("v2"), 3600, ""); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := e.Delete([]byte("k1"), ""); err != nil {
		t.Fatalf("delete: %v", err)
	}

	snap := e.Snapshot()
	if len(snap) != 1 || string(snap[0].Key) != "k2" || string(snap[0].Value) != "v2" {
		t.Fatalf("expected snapshot to contain only live key k2, got %+v", snap)
	}
	if snap[0].ExpiresAtUnixNano == 0 {
		t.Fatal("expected snapshot entry to preserve TTL expiry")
	}

	if err := e.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, _, found := e.Get([]byte("k2")); found {
		t.Fatal("expected engine to be empty immediately after reset")
	}

	for _, entry := range snap {
		if _, err := e.ApplyPut(entry.Key, entry.Value, entry.ExpiresAtUnixNano, ""); err != nil {
			t.Fatalf("apply put during reload: %v", err)
		}
	}

	value, _, found := e.Get([]byte("k2"))
	if !found || string(value) != "v2" {
		t.Fatalf("expected k2=v2 after reload, got value=%q found=%v", value, found)
	}
}

func TestResetSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reset.wal")
	e, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	if _, err := e.Put([]byte("stale"), []byte("v"), 0, ""); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := e.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := e.Put([]byte("fresh"), []byte("v"), 0, ""); err != nil {
		t.Fatalf("put after reset: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	e2, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()

	if _, _, found := e2.Get([]byte("stale")); found {
		t.Fatal("stale pre-reset key must not survive restart")
	}
	if _, _, found := e2.Get([]byte("fresh")); !found {
		t.Fatal("post-reset key must survive restart")
	}
}
