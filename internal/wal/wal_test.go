package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	pos1, err := w.Append(Record{Kind: KindPut, Key: []byte("a"), Value: []byte("1")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	pos2, err := w.Append(Record{Kind: KindPut, Key: []byte("b"), Value: []byte("2")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if pos1 != 1 || pos2 != 2 {
		t.Fatalf("expected monotonic positions 1,2; got %d,%d", pos1, pos2)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var replayed []Record
	w2, err := Open(path, func(r Record) error {
		replayed = append(replayed, r)
		return nil
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	if len(replayed) != 2 {
		t.Fatalf("expected 2 replayed records, got %d", len(replayed))
	}
	if string(replayed[0].Key) != "a" || string(replayed[1].Key) != "b" {
		t.Fatalf("unexpected replay order: %+v", replayed)
	}

	// A fresh append after reopen must continue the position sequence.
	pos3, err := w2.Append(Record{Kind: KindPut, Key: []byte("c"), Value: []byte("3")})
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if pos3 != 3 {
		t.Fatalf("expected position 3 after reopen, got %d", pos3)
	}
}

func TestRecoveryTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "torn.wal")

	w, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := w.Append(Record{Kind: KindPut, Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	fullSize, err := fileSize(path)
	if err != nil {
		t.Fatal(err)
	}

	// Append a second record, then truncate the file mid-write to simulate
	// a crash during an fsync-before-return append (the torn tail case
	// TR-005 requires recovery to tolerate).
	w2, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := w2.Append(Record{Kind: KindPut, Key: []byte("b"), Value: []byte("2")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	fullSizeWithB, err := fileSize(path)
	if err != nil {
		t.Fatal(err)
	}
	tornSize := fullSize + (fullSizeWithB-fullSize)/2
	if err := os.Truncate(path, tornSize); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	var replayed []Record
	w3, err := Open(path, func(r Record) error {
		replayed = append(replayed, r)
		return nil
	})
	if err != nil {
		t.Fatalf("recovery on torn tail should succeed, got: %v", err)
	}
	defer w3.Close()

	if len(replayed) != 1 || string(replayed[0].Key) != "a" {
		t.Fatalf("expected only record 'a' to survive torn tail, got %+v", replayed)
	}

	pos, err := w3.Append(Record{Kind: KindPut, Key: []byte("c"), Value: []byte("3")})
	if err != nil {
		t.Fatalf("append after torn recovery: %v", err)
	}
	if pos != 2 {
		t.Fatalf("expected next position 2 after truncation, got %d", pos)
	}
}

func TestRecoveryRejectsCorruptChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.wal")

	w, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := w.Append(Record{Kind: KindPut, Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := w.Append(Record{Kind: KindPut, Key: []byte("b"), Value: []byte("2")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Flip a byte inside the first record's payload region (well past the
	// length/crc header) to simulate media corruption rather than a torn
	// write; recovery must fail hard rather than silently skip it.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, 20); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := Open(path, nil); err == nil {
		t.Fatal("expected recovery to fail on corrupted checksum")
	}
}

func fileSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
