package streaming

import (
	"path/filepath"
	"testing"
)

// Engineering-regression benchmark, not the TR-019 benchmark harness — see
// internal/wal/wal_bench_test.go for why that distinction matters here.
func BenchmarkCommit(b *testing.B) {
	s, err := Open(filepath.Join(b.TempDir(), "bench.wal"))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer s.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Commit("group", uint64(i+1)); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}
