package benchmark

import (
	"testing"
	"time"
)

func TestRecorderDiscardsSamplesBeforeWarmup(t *testing.T) {
	r := NewRecorder()
	r.Record(OpGet, 10*time.Millisecond, nil)
	r.SetWarmedUp()
	r.Record(OpGet, 20*time.Millisecond, nil)

	stats := r.Compute(time.Second)[OpGet]
	if stats.Count != 1 {
		t.Fatalf("expected only the post-warmup sample to be recorded, got count=%d", stats.Count)
	}
	if stats.P50 != 20*time.Millisecond {
		t.Fatalf("expected the recorded sample to be the post-warmup one, got p50=%v", stats.P50)
	}
}

func TestRecorderPercentiles(t *testing.T) {
	r := NewRecorder()
	r.SetWarmedUp()
	// 100 samples: 1ms, 2ms, ..., 100ms.
	for i := 1; i <= 100; i++ {
		r.Record(OpGet, time.Duration(i)*time.Millisecond, nil)
	}

	stats := r.Compute(time.Second)[OpGet]
	if stats.Count != 100 {
		t.Fatalf("expected 100 samples, got %d", stats.Count)
	}
	if stats.Min != 1*time.Millisecond || stats.Max != 100*time.Millisecond {
		t.Fatalf("unexpected min/max: min=%v max=%v", stats.Min, stats.Max)
	}
	// Nearest-rank at p50 of 100 sorted samples (index 50, 0-based) is the
	// 51st smallest value = 51ms.
	if stats.P50 != 51*time.Millisecond {
		t.Fatalf("expected p50=51ms, got %v", stats.P50)
	}
	if stats.P99 != 100*time.Millisecond {
		t.Fatalf("expected p99=100ms (clamped to max), got %v", stats.P99)
	}
}

func TestRecorderTracksErrorsSeparately(t *testing.T) {
	r := NewRecorder()
	r.SetWarmedUp()
	r.Record(OpPut, 5*time.Millisecond, nil)
	r.Record(OpPut, 0, errFake)
	r.Record(OpPut, 0, errFake)

	stats := r.Compute(time.Second)[OpPut]
	if stats.Count != 1 {
		t.Fatalf("expected 1 successful sample, got %d", stats.Count)
	}
	if stats.Errors != 2 {
		t.Fatalf("expected 2 errors, got %d", stats.Errors)
	}
}

func TestThroughputIncludesErrors(t *testing.T) {
	stats := OpStats{Count: 8, Errors: 2}
	tp := stats.Throughput(2 * time.Second)
	if tp != 5.0 {
		t.Fatalf("expected throughput (8+2)/2s = 5.0, got %v", tp)
	}
}

var errFake = fakeErr{}

type fakeErr struct{}

func (fakeErr) Error() string { return "fake error" }
