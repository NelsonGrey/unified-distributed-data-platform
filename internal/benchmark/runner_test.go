package benchmark

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakeExecutor struct {
	calls     int64
	failEvery int64 // 0 = never fail
}

func (f *fakeExecutor) Execute(ctx context.Context, req Request) error {
	n := atomic.AddInt64(&f.calls, 1)
	if f.failEvery > 0 && n%f.failEvery == 0 {
		return errors.New("injected failure")
	}
	return nil
}

func TestRunExcludesWarmupAndRespectsDuration(t *testing.T) {
	exec := &fakeExecutor{}
	cfg := RunConfig{
		Workload:    WorkloadConfig{ReadRatio: 1.0, KeyCardinality: 10, ValueSize: 8},
		Concurrency: 4,
		Warmup:      50 * time.Millisecond,
		Duration:    150 * time.Millisecond,
	}

	start := time.Now()
	result := Run(context.Background(), cfg, exec)
	elapsed := time.Since(start)

	if elapsed < cfg.Warmup+cfg.Duration {
		t.Fatalf("run returned before warmup+duration elapsed: %v", elapsed)
	}

	total := 0
	for _, s := range result.Stats {
		total += s.Count + s.Errors
	}
	if total == 0 {
		t.Fatal("expected some recorded operations after warmup")
	}
	if exec.calls == 0 {
		t.Fatal("expected the executor to have been called")
	}
	// Every call happened (during warmup + measured), but only
	// post-warmup ones should be recorded — so recorded count should be
	// less than total executor calls (unless warmup did essentially
	// nothing, which isn't the case with 4 concurrent workers).
	if int64(total) > exec.calls {
		t.Fatalf("recorded more operations (%d) than the executor was called (%d)", total, exec.calls)
	}
}

func TestRunRecordsErrors(t *testing.T) {
	exec := &fakeExecutor{failEvery: 3}
	cfg := RunConfig{
		Workload:    WorkloadConfig{ReadRatio: 0.0, KeyCardinality: 10, ValueSize: 8},
		Concurrency: 2,
		Warmup:      10 * time.Millisecond,
		Duration:    100 * time.Millisecond,
	}

	result := Run(context.Background(), cfg, exec)
	stats := result.Stats[OpPut]
	if stats.Errors == 0 {
		t.Fatal("expected some recorded errors given failEvery=3")
	}
}

func TestRunRespectsContextCancellation(t *testing.T) {
	exec := &fakeExecutor{}
	cfg := RunConfig{
		Workload:    WorkloadConfig{ReadRatio: 1.0, KeyCardinality: 10, ValueSize: 8},
		Concurrency: 2,
		Warmup:      time.Hour, // would hang forever without cancellation
		Duration:    time.Hour,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		Run(ctx, cfg, exec)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not respect context cancellation")
	}
}
