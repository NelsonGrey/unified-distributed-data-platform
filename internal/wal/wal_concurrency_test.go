package wal

import (
	"path/filepath"
	"sync"
	"testing"
)

// TestConcurrentAppendGroupCommit is the correctness check for group commit
// (see Append's doc comment): concurrent callers must still get unique,
// monotonic commit positions, and every one of them must be durable — not
// just accepted — by the time Append returns, whether they led their own
// fsync or rode along on someone else's.
func TestConcurrentAppendGroupCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.wal")
	w, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	const goroutines = 50
	const perGoroutine = 20

	var wg sync.WaitGroup
	positions := make(chan uint64, goroutines*perGoroutine)
	errs := make(chan error, goroutines*perGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				pos, err := w.Append(Record{Kind: KindPut, Key: []byte("k"), Value: []byte("v")})
				if err != nil {
					errs <- err
					continue
				}
				positions <- pos
			}
		}(g)
	}
	wg.Wait()
	close(positions)
	close(errs)

	for err := range errs {
		t.Fatalf("append error: %v", err)
	}

	seen := make(map[uint64]bool)
	for pos := range positions {
		if seen[pos] {
			t.Fatalf("duplicate commit position %d", pos)
		}
		seen[pos] = true
	}
	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("expected %d unique positions, got %d", goroutines*perGoroutine, len(seen))
	}
	for i := uint64(1); i <= uint64(goroutines*perGoroutine); i++ {
		if !seen[i] {
			t.Fatalf("missing commit position %d — positions must be contiguous", i)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Recovery must reproduce exactly what was durably appended, in order.
	var replayed int
	var lastPos uint64
	w2, err := Open(path, func(r Record) error {
		replayed++
		if r.CommitPosition != lastPos+1 {
			t.Fatalf("non-contiguous replay: got %d after %d", r.CommitPosition, lastPos)
		}
		lastPos = r.CommitPosition
		return nil
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	if replayed != goroutines*perGoroutine {
		t.Fatalf("expected %d records on replay, got %d", goroutines*perGoroutine, replayed)
	}
}
