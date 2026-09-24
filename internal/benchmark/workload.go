// Package benchmark implements the performance qualification harness from
// TR-019: environment/workload/percentile capture for the KV surface. It
// does NOT by itself make any result a publishable performance claim
// (BR-010, TRD §9) — that needs a fixed, approved environment and multiple
// repetitions, which is an operational discipline around how this tool is
// run, not something the tool can enforce. What this package covers:
// read/write/mixed/TTL-churn/hot-key workloads, warmup exclusion, and
// p50/p95/p99/p99.9 latency plus throughput and error rate.
//
// Explicitly deferred (TRD §9's full qualification list): automatic
// saturation-point detection, and running the workload concurrently with
// injected node/zone failure, leader change, snapshot, rebalance, upgrade,
// or backup/restore. Those need the workload generator to run alongside a
// separate fault-injection driver — a real piece of work, not a flag on
// this tool — and are left for a later slice rather than half-built here.
package benchmark

import (
	"math/rand"
)

// Op identifies which StateService RPC a generated request exercises.
type Op int

const (
	OpGet Op = iota
	OpPut
)

// Request is one generated operation for the runner to execute.
type Request struct {
	Op         Op
	Key        []byte
	Value      []byte
	TTLSeconds int64
}

// WorkloadConfig parameterizes request generation.
type WorkloadConfig struct {
	// ReadRatio is the fraction of requests that are Get (0.0 = all
	// writes, 1.0 = all reads).
	ReadRatio float64
	// KeyCardinality is the number of distinct keys in the working set.
	KeyCardinality int
	// HotKeyCount, if > 0, makes the top HotKeyCount keys receive 80% of
	// traffic (the standard 80/20 hot-key pattern) instead of uniform
	// selection across KeyCardinality.
	HotKeyCount int
	// ValueSize is the size in bytes of generated values.
	ValueSize int
	// TTLSeconds, if > 0, is set on every write, producing a TTL-churn
	// workload (short TTLs cause continuous expiry) when set low relative
	// to the run duration.
	TTLSeconds int64
}

// Generator produces Requests according to a WorkloadConfig. It is not
// safe for concurrent use — each worker goroutine should have its own
// Generator (seeded independently) rather than share one.
type Generator struct {
	cfg   WorkloadConfig
	rng   *rand.Rand
	value []byte
}

// NewGenerator returns a Generator seeded from seed (callers should pass a
// distinct seed per worker to avoid identical request sequences).
func NewGenerator(cfg WorkloadConfig, seed int64) *Generator {
	value := make([]byte, cfg.ValueSize)
	rand.New(rand.NewSource(seed)).Read(value)
	return &Generator{cfg: cfg, rng: rand.New(rand.NewSource(seed)), value: value}
}

// Next returns the next generated request.
func (g *Generator) Next() Request {
	key := g.key()
	if g.rng.Float64() < g.cfg.ReadRatio {
		return Request{Op: OpGet, Key: key}
	}
	return Request{Op: OpPut, Key: key, Value: g.value, TTLSeconds: g.cfg.TTLSeconds}
}

func (g *Generator) key() []byte {
	n := g.cfg.KeyCardinality
	if n <= 0 {
		n = 1
	}

	var i int
	if g.cfg.HotKeyCount > 0 && g.cfg.HotKeyCount < n {
		// 80% of requests land on the hot set, 20% spread across the rest.
		if g.rng.Float64() < 0.8 {
			i = g.rng.Intn(g.cfg.HotKeyCount)
		} else {
			i = g.cfg.HotKeyCount + g.rng.Intn(n-g.cfg.HotKeyCount)
		}
	} else {
		i = g.rng.Intn(n)
	}

	return formatKey(i)
}

func formatKey(i int) []byte {
	const hex = "0123456789abcdef"
	// Fixed-width hex key, cheaper than fmt.Sprintf in a hot loop and
	// avoids repeated allocations from string formatting.
	b := make([]byte, 9)
	copy(b, "k-")
	for p := 8; p >= 2; p-- {
		b[p] = hex[i&0xf]
		i >>= 4
	}
	return b
}
