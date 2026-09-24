package streaming

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestCommitAndFetch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if _, found := s.Fetch("g1"); found {
		t.Fatal("expected no offset before first commit")
	}

	if _, err := s.Commit("g1", 5); err != nil {
		t.Fatalf("commit: %v", err)
	}
	offset, found := s.Fetch("g1")
	if !found || offset != 5 {
		t.Fatalf("expected offset 5, got %d (found=%v)", offset, found)
	}

	if _, err := s.Commit("g1", 3); !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("expected ErrOffsetRegression, got %v", err)
	}
	offset, _ = s.Fetch("g1")
	if offset != 5 {
		t.Fatalf("regression attempt must not change stored offset, got %d", offset)
	}
}

func TestOffsetsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.wal")

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s1.Commit("g1", 10); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := s1.Commit("g2", 20); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	if offset, found := s2.Fetch("g1"); !found || offset != 10 {
		t.Fatalf("expected g1 offset 10 after restart, got %d (found=%v)", offset, found)
	}
	if offset, found := s2.Fetch("g2"); !found || offset != 20 {
		t.Fatalf("expected g2 offset 20 after restart, got %d (found=%v)", offset, found)
	}
}
