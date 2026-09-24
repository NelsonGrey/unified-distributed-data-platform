package engine

import (
	"fmt"
	"path/filepath"
	"testing"
)

// Engineering-regression benchmarks, not the TR-019 benchmark harness — see
// internal/wal/wal_bench_test.go for why that distinction matters here.

func BenchmarkPut(b *testing.B) {
	e, err := Open(filepath.Join(b.TempDir(), "bench.wal"), nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer e.Close()

	key := []byte("benchmark-key")
	value := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Put(key, value, 0, ""); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	e, err := Open(filepath.Join(b.TempDir(), "bench.wal"), nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer e.Close()

	key := []byte("benchmark-key")
	if _, err := e.Put(key, make([]byte, 256), 0, ""); err != nil {
		b.Fatalf("put: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, found := e.Get(key); !found {
			b.Fatal("expected key to be found")
		}
	}
}

func BenchmarkCompareAndSet(b *testing.B) {
	e, err := Open(filepath.Join(b.TempDir(), "bench.wal"), nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer e.Close()

	key := []byte("benchmark-key")
	value := make([]byte, 256)
	var version uint64

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := e.CompareAndSet(key, value, version, 0, "")
		if err != nil {
			b.Fatalf("cas: %v", err)
		}
		version = out.Version
	}
}

func BenchmarkFetch(b *testing.B) {
	e, err := Open(filepath.Join(b.TempDir(), "bench.wal"), nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer e.Close()

	const records = 2_000
	value := make([]byte, 256)
	for i := 0; i < records; i++ {
		if _, err := e.Put([]byte(fmt.Sprintf("k%d", i)), value, 0, ""); err != nil {
			b.Fatalf("put: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Fetch(1, 100); err != nil {
			b.Fatalf("fetch: %v", err)
		}
	}
}
