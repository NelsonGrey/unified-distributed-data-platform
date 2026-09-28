package wal

import (
	"os"
	"path/filepath"
	"testing"
)

// TestOpenRejectsUnsupportedFormatVersion is the concrete mechanism behind
// BR-011/TR-018: a version this build doesn't understand must fail loudly
// at Open, not be silently read (possibly incorrectly) or ignored.
func TestOpenRejectsUnsupportedFormatVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.wal")

	w, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := w.Append(Record{Kind: KindPut, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrupt the version byte to simulate a future/unknown format.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{currentFormatVersion + 1}, 4); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := Open(path, nil); err == nil {
		t.Fatal("expected Open to reject an unsupported format version")
	}
}

func TestOpenRejectsBadMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notawal.wal")
	if err := os.WriteFile(path, []byte("not a uddp wal file at all"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, nil); err == nil {
		t.Fatal("expected Open to reject a file with the wrong magic")
	}
}

func TestLastCommitPosition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pos.wal")
	w, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	if got := w.LastCommitPosition(); got != 0 {
		t.Fatalf("expected 0 for an empty log, got %d", got)
	}
	if _, err := w.Append(Record{Kind: KindPut, Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := w.Append(Record{Kind: KindPut, Key: []byte("b"), Value: []byte("2")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := w.LastCommitPosition(); got != 2 {
		t.Fatalf("expected 2 after two appends, got %d", got)
	}
}
