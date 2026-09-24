package wal

import (
	"path/filepath"
	"testing"
)

// These are engineering-regression benchmarks (catch a change that
// accidentally makes Append or ReadRange N times slower), not the
// TR-019 benchmark harness — that requires a fixed environment manifest,
// multiple repetitions, and percentile/tail-latency reporting before any
// number here could back a public performance claim (BR-010, TRD 9).
func BenchmarkAppend(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.wal")
	w, err := Open(path, nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer w.Close()

	key := []byte("benchmark-key")
	value := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Append(Record{Kind: KindPut, Key: key, Value: value}); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

func BenchmarkReadRange(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.wal")
	w, err := Open(path, nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer w.Close()

	const records = 2_000
	value := make([]byte, 256)
	for i := 0; i < records; i++ {
		if _, err := w.Append(Record{Kind: KindPut, Key: []byte("k"), Value: value}); err != nil {
			b.Fatalf("append: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.ReadRange(1, 100); err != nil {
			b.Fatalf("read range: %v", err)
		}
	}
}
