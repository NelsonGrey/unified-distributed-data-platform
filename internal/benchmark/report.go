package benchmark

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"time"
)

// Environment captures the fields TR-019 requires alongside a workload
// result. This is the *client's* environment (where uddp-bench itself
// ran), not the server's — the server side (hardware, topology, uddp-node
// version, durability profile) has to be supplied by the caller (via
// --target-note or similar), since this tool has no way to introspect a
// remote node's hardware from here. A benchmark report missing that
// context is exactly the kind of "successful request treated as evidence"
// TRACEABILITY_AND_VALIDATION.md §1 warns against — fill it in.
type Environment struct {
	Timestamp  time.Time `json:"timestamp"`
	GoVersion  string    `json:"go_version"`
	GOOS       string    `json:"goos"`
	GOARCH     string    `json:"goarch"`
	NumCPU     int       `json:"num_cpu"`
	Target     string    `json:"target"`
	TargetNote string    `json:"target_note,omitempty"`
}

// CaptureEnvironment fills in what this process can observe about itself.
func CaptureEnvironment(target, targetNote string) Environment {
	return Environment{
		Timestamp:  time.Now().UTC(),
		GoVersion:  runtime.Version(),
		GOOS:       runtime.GOOS,
		GOARCH:     runtime.GOARCH,
		NumCPU:     runtime.NumCPU(),
		Target:     target,
		TargetNote: targetNote,
	}
}

// Report is the full TR-019 record for one run: environment, the workload
// config that was actually driven, warmup/duration, and per-operation
// percentiles/throughput/errors.
type Report struct {
	Environment Environment         `json:"environment"`
	Workload    WorkloadConfig      `json:"workload"`
	Concurrency int                 `json:"concurrency"`
	Warmup      time.Duration       `json:"warmup_ns"`
	Duration    time.Duration       `json:"duration_ns"`
	Measured    time.Duration       `json:"measured_ns"`
	Ops         map[string]OpReport `json:"ops"`
}

// OpReport is OpStats plus derived throughput, with duration fields in
// nanoseconds (json-friendly, exact) rather than time.Duration's default
// string form.
type OpReport struct {
	Count      int     `json:"count"`
	Errors     int     `json:"errors"`
	Throughput float64 `json:"throughput_ops_per_sec"`
	P50Ns      int64   `json:"p50_ns"`
	P95Ns      int64   `json:"p95_ns"`
	P99Ns      int64   `json:"p99_ns"`
	P999Ns     int64   `json:"p999_ns"`
	MinNs      int64   `json:"min_ns"`
	MaxNs      int64   `json:"max_ns"`
	MeanNs     int64   `json:"mean_ns"`
}

func opName(op Op) string {
	switch op {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	default:
		return "unknown"
	}
}

// NewReport assembles a Report from a Result and the environment it ran
// in.
func NewReport(env Environment, result Result) Report {
	ops := make(map[string]OpReport, len(result.Stats))
	for op, s := range result.Stats {
		ops[opName(op)] = OpReport{
			Count: s.Count, Errors: s.Errors,
			Throughput: s.Throughput(result.MeasuredDuration),
			P50Ns:      int64(s.P50), P95Ns: int64(s.P95), P99Ns: int64(s.P99), P999Ns: int64(s.P999),
			MinNs: int64(s.Min), MaxNs: int64(s.Max), MeanNs: int64(s.Mean),
		}
	}
	return Report{
		Environment: env,
		Workload:    result.Config.Workload,
		Concurrency: result.Config.Concurrency,
		Warmup:      result.Config.Warmup,
		Duration:    result.Config.Duration,
		Measured:    result.MeasuredDuration,
		Ops:         ops,
	}
}

// WriteJSON writes the report as indented JSON.
func (r Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteText writes a compact human-readable summary table.
func (r Report) WriteText(w io.Writer) error {
	fmt.Fprintf(w, "target=%s (%s)\n", r.Environment.Target, r.Environment.TargetNote)
	fmt.Fprintf(w, "workload: read_ratio=%.2f key_cardinality=%d hot_key_count=%d value_size=%d ttl_seconds=%d\n",
		r.Workload.ReadRatio, r.Workload.KeyCardinality, r.Workload.HotKeyCount, r.Workload.ValueSize, r.Workload.TTLSeconds)
	fmt.Fprintf(w, "concurrency=%d warmup=%s measured=%s\n\n", r.Concurrency, r.Warmup, r.Measured)
	fmt.Fprintf(w, "%-6s %10s %8s %12s %10s %10s %10s %10s\n", "op", "count", "errors", "throughput", "p50", "p95", "p99", "p99.9")
	for name, op := range r.Ops {
		fmt.Fprintf(w, "%-6s %10d %8d %9.1f/s %10s %10s %10s %10s\n",
			name, op.Count, op.Errors, op.Throughput,
			time.Duration(op.P50Ns), time.Duration(op.P95Ns), time.Duration(op.P99Ns), time.Duration(op.P999Ns))
	}
	return nil
}
