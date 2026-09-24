package engine

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func openTest(t *testing.T) (*Engine, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "partition-0.wal")
	e, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e, path
}

func TestPutGetRoundTrip(t *testing.T) {
	e, _ := openTest(t)

	if _, _, found := e.Get([]byte("k")); found {
		t.Fatal("expected key absent before put")
	}

	out, err := e.Put([]byte("k"), []byte("v1"), 0, "")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if out.CommitPosition != 1 {
		t.Fatalf("expected commit position 1, got %d", out.CommitPosition)
	}

	value, version, found := e.Get([]byte("k"))
	if !found || string(value) != "v1" || version != 1 {
		t.Fatalf("unexpected get result: value=%s version=%d found=%v", value, version, found)
	}
}

func TestRecoveryReplaysStateAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partition-0.wal")

	e1, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := e1.Put([]byte("k"), []byte("v1"), 0, ""); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := e1.Put([]byte("k"), []byte("v2"), 0, ""); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := e1.Delete([]byte("other"), ""); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	e2, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()

	value, version, found := e2.Get([]byte("k"))
	if !found || string(value) != "v2" || version != 2 {
		t.Fatalf("recovery did not reconstruct latest state: value=%s version=%d found=%v", value, version, found)
	}

	// Next mutation must continue the commit position sequence, not
	// restart from 1, since positions are the partition's durable order.
	out, err := e2.Put([]byte("k2"), []byte("v"), 0, "")
	if err != nil {
		t.Fatalf("put after recovery: %v", err)
	}
	if out.CommitPosition != 4 {
		t.Fatalf("expected commit position 4 after recovery, got %d", out.CommitPosition)
	}
}

func TestIdempotentRetryDoesNotDuplicateCommit(t *testing.T) {
	e, _ := openTest(t)

	out1, err := e.Put([]byte("k"), []byte("v1"), 0, "req-1")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if out1.Deduplicated {
		t.Fatal("first request should not be marked deduplicated")
	}

	out2, err := e.Put([]byte("k"), []byte("v2-should-be-ignored"), 0, "req-1")
	if err != nil {
		t.Fatalf("retry put: %v", err)
	}
	if !out2.Deduplicated || out2.CommitPosition != out1.CommitPosition {
		t.Fatalf("retry with same idempotency key should return original outcome, got %+v", out2)
	}

	value, _, _ := e.Get([]byte("k"))
	if string(value) != "v1" {
		t.Fatalf("retried mutation must not apply again; got value %q", value)
	}
}

func TestCompareAndSetConflict(t *testing.T) {
	e, _ := openTest(t)

	if _, err := e.Put([]byte("k"), []byte("v1"), 0, ""); err != nil {
		t.Fatalf("put: %v", err)
	}

	_, err := e.CompareAndSet([]byte("k"), []byte("v2"), 999, 0, "")
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	value, _, _ := e.Get([]byte("k"))
	if string(value) != "v1" {
		t.Fatalf("failed CAS must not mutate state; got %q", value)
	}

	_, version, _ := e.Get([]byte("k"))
	out, err := e.CompareAndSet([]byte("k"), []byte("v2"), version, 0, "")
	if err != nil {
		t.Fatalf("expected CAS with correct version to succeed: %v", err)
	}
	value, _, _ = e.Get([]byte("k"))
	if string(value) != "v2" || out.Version == 0 {
		t.Fatalf("CAS with matching version should apply; got value=%q outcome=%+v", value, out)
	}
}

func TestTTLExpiryUsesInjectedClock(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	path := filepath.Join(t.TempDir(), "ttl.wal")
	e, err := Open(path, clock)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	if _, err := e.Put([]byte("k"), []byte("v"), 10, ""); err != nil {
		t.Fatalf("put: %v", err)
	}

	if _, _, found := e.Get([]byte("k")); !found {
		t.Fatal("expected key present before expiry")
	}

	clock.now = clock.now.Add(11 * time.Second)
	if _, _, found := e.Get([]byte("k")); found {
		t.Fatal("expected key expired after TTL elapsed")
	}
}
