package engine

import "testing"

// TestFetchMirrorsStateCommits is the direct check for BR-003: every
// committed Put/Delete produces exactly one change record at the same
// offset as its state version, with no separate dual-write path.
func TestFetchMirrorsStateCommits(t *testing.T) {
	e, _ := openTest(t)

	putOut, err := e.Put([]byte("k1"), []byte("v1"), 0, "")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	delOut, err := e.Delete([]byte("k2"), "")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	recs, err := e.Fetch(1, 10)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 change records, got %d: %+v", len(recs), recs)
	}

	if recs[0].Offset != putOut.Version || string(recs[0].Key) != "k1" || string(recs[0].Value) != "v1" {
		t.Fatalf("put change record mismatch: %+v (want version %d)", recs[0], putOut.Version)
	}
	if recs[1].Offset != delOut.Version || string(recs[1].Key) != "k2" {
		t.Fatalf("delete change record mismatch: %+v (want version %d)", recs[1], delOut.Version)
	}
}

func TestFetchPastEndReturnsEmpty(t *testing.T) {
	e, _ := openTest(t)

	if _, err := e.Put([]byte("k"), []byte("v"), 0, ""); err != nil {
		t.Fatalf("put: %v", err)
	}

	recs, err := e.Fetch(5, 10)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected no records past end, got %+v", recs)
	}
}
