package benchmark

import (
	"context"
	"sync"
	"time"
)

// Executor performs one generated Request against a target (a real
// StateServiceClient in cmd/uddp-bench, a fake in tests).
type Executor interface {
	Execute(ctx context.Context, req Request) error
}

// RunConfig controls one benchmark run.
type RunConfig struct {
	Workload    WorkloadConfig
	Concurrency int
	Warmup      time.Duration
	Duration    time.Duration
}

// Result is everything TR-019 asks a benchmark run to capture about its
// workload and measured outcome (environment/topology are captured
// separately at the cmd/uddp-bench layer, which knows about the target
// connection — this package only knows about the workload it drove).
type Result struct {
	Config           RunConfig
	MeasuredDuration time.Duration
	Stats            map[Op]OpStats
}

// Run drives cfg.Concurrency workers against executor for cfg.Warmup +
// cfg.Duration, discarding samples recorded during warmup, and returns the
// measured results. It respects ctx cancellation (e.g. Ctrl-C) as an early
// stop, in which case MeasuredDuration reflects the actual elapsed
// measured time, not the requested cfg.Duration.
func Run(ctx context.Context, cfg RunConfig, executor Executor) Result {
	rec := NewRecorder()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			gen := NewGenerator(cfg.Workload, seed)
			for {
				select {
				case <-stop:
					return
				case <-ctx.Done():
					return
				default:
				}
				req := gen.Next()
				start := time.Now()
				err := executor.Execute(ctx, req)
				rec.Record(req.Op, time.Since(start), err)
			}
		}(int64(i) + 1)
	}

	warmupTimer := time.NewTimer(cfg.Warmup)
	var measuredStart time.Time
	select {
	case <-warmupTimer.C:
		measuredStart = time.Now()
		rec.SetWarmedUp()
	case <-ctx.Done():
		warmupTimer.Stop()
		close(stop)
		wg.Wait()
		return Result{Config: cfg, MeasuredDuration: 0, Stats: rec.Compute(0)}
	}

	durationTimer := time.NewTimer(cfg.Duration)
	select {
	case <-durationTimer.C:
	case <-ctx.Done():
		durationTimer.Stop()
	}

	measured := time.Since(measuredStart)
	close(stop)
	wg.Wait()

	return Result{Config: cfg, MeasuredDuration: measured, Stats: rec.Compute(measured)}
}
