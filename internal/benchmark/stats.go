package benchmark

import (
	"sort"
	"sync"
	"time"
)

// Recorder collects per-operation latency samples and error counts during
// a benchmark run. Safe for concurrent use by multiple worker goroutines.
//
// Samples are kept in memory and sorted at Compute time for exact
// percentiles, rather than an approximate streaming quantile structure —
// simpler and exact, at the cost of O(n) memory for the run. Documented
// simplification: a multi-hour, very-high-QPS run would want a streaming
// approach (e.g. a t-digest) instead; this is fine for the run lengths
// this harness targets.
type Recorder struct {
	mu       sync.Mutex
	samples  map[Op][]time.Duration
	errors   map[Op]int
	warmedUp bool
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{
		samples: make(map[Op][]time.Duration),
		errors:  make(map[Op]int),
	}
}

// SetWarmedUp marks the recorder as past warmup; Record calls before this
// is set are discarded, per TR-019's requirement to capture "warmup"
// separately from measured results.
func (r *Recorder) SetWarmedUp() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warmedUp = true
}

// Record adds one sample. err non-nil counts as an error for op and is not
// included in latency percentiles (an errored call's duration isn't a
// meaningful latency sample for the operation it failed to perform).
func (r *Recorder) Record(op Op, d time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.warmedUp {
		return
	}
	if err != nil {
		r.errors[op]++
		return
	}
	r.samples[op] = append(r.samples[op], d)
}

// OpStats summarizes one operation's measured results.
type OpStats struct {
	Count               int
	Errors              int
	P50, P95, P99, P999 time.Duration
	Min, Max            time.Duration
	Mean                time.Duration
}

// Compute returns a snapshot of results per operation, plus overall
// throughput given the measured (post-warmup) duration.
func (r *Recorder) Compute(measuredDuration time.Duration) map[Op]OpStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make(map[Op]OpStats, len(r.samples)+len(r.errors))
	ops := make(map[Op]bool)
	for op := range r.samples {
		ops[op] = true
	}
	for op := range r.errors {
		ops[op] = true
	}

	for op := range ops {
		samples := append([]time.Duration(nil), r.samples[op]...)
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

		stats := OpStats{Count: len(samples), Errors: r.errors[op]}
		if len(samples) > 0 {
			var sum time.Duration
			for _, s := range samples {
				sum += s
			}
			stats.Mean = sum / time.Duration(len(samples))
			stats.Min = samples[0]
			stats.Max = samples[len(samples)-1]
			stats.P50 = percentile(samples, 0.50)
			stats.P95 = percentile(samples, 0.95)
			stats.P99 = percentile(samples, 0.99)
			stats.P999 = percentile(samples, 0.999)
		}
		out[op] = stats
	}
	return out
}

// percentile returns the value at p (0..1) in a pre-sorted slice using
// nearest-rank; sorted must be non-empty.
func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Throughput returns ops/sec for a stats snapshot given the measured
// window (count / seconds), including errored attempts in the rate since
// they still consumed the window's time — TR-019 wants errors reported
// alongside throughput, not hidden by excluding them from it.
func (s OpStats) Throughput(measuredDuration time.Duration) float64 {
	total := s.Count + s.Errors
	if measuredDuration <= 0 || total == 0 {
		return 0
	}
	return float64(total) / measuredDuration.Seconds()
}
