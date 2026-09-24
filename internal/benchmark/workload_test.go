package benchmark

import "testing"

func TestGeneratorReadRatio(t *testing.T) {
	gen := NewGenerator(WorkloadConfig{ReadRatio: 1.0, KeyCardinality: 100, ValueSize: 8}, 1)
	for i := 0; i < 1000; i++ {
		if req := gen.Next(); req.Op != OpGet {
			t.Fatalf("expected all-read workload to produce only Get, got op %v at iteration %d", req.Op, i)
		}
	}

	gen = NewGenerator(WorkloadConfig{ReadRatio: 0.0, KeyCardinality: 100, ValueSize: 8}, 1)
	for i := 0; i < 1000; i++ {
		if req := gen.Next(); req.Op != OpPut {
			t.Fatalf("expected all-write workload to produce only Put, got op %v at iteration %d", req.Op, i)
		}
	}
}

func TestGeneratorMixedRatioIsApproximatelyCorrect(t *testing.T) {
	gen := NewGenerator(WorkloadConfig{ReadRatio: 0.3, KeyCardinality: 100, ValueSize: 8}, 42)
	var reads int
	const n = 100000
	for i := 0; i < n; i++ {
		if gen.Next().Op == OpGet {
			reads++
		}
	}
	ratio := float64(reads) / n
	if ratio < 0.28 || ratio > 0.32 {
		t.Fatalf("expected read ratio near 0.30 over %d samples, got %.3f", n, ratio)
	}
}

func TestGeneratorKeyCardinalityRespected(t *testing.T) {
	gen := NewGenerator(WorkloadConfig{ReadRatio: 0.0, KeyCardinality: 10, ValueSize: 8}, 7)
	seen := make(map[string]bool)
	for i := 0; i < 5000; i++ {
		seen[string(gen.Next().Key)] = true
	}
	if len(seen) != 10 {
		t.Fatalf("expected exactly 10 distinct keys with KeyCardinality=10, got %d", len(seen))
	}
}

func TestGeneratorHotKeyConcentration(t *testing.T) {
	gen := NewGenerator(WorkloadConfig{ReadRatio: 0.0, KeyCardinality: 1000, HotKeyCount: 10, ValueSize: 8}, 3)
	hotSet := make(map[string]bool)
	for i := 0; i < 10; i++ {
		hotSet[string(formatKey(i))] = true
	}

	var hotHits int
	const n = 100000
	for i := 0; i < n; i++ {
		if hotSet[string(gen.Next().Key)] {
			hotHits++
		}
	}
	ratio := float64(hotHits) / n
	if ratio < 0.75 || ratio > 0.85 {
		t.Fatalf("expected ~80%% of traffic on the 10-key hot set, got %.3f", ratio)
	}
}

func TestGeneratorTTLAppliedToWrites(t *testing.T) {
	gen := NewGenerator(WorkloadConfig{ReadRatio: 0.0, KeyCardinality: 10, ValueSize: 8, TTLSeconds: 30}, 1)
	req := gen.Next()
	if req.TTLSeconds != 30 {
		t.Fatalf("expected TTL 30 on generated write, got %d", req.TTLSeconds)
	}
}

func TestFormatKeyDistinctForDistinctInputs(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		k := string(formatKey(i))
		if seen[k] {
			t.Fatalf("formatKey(%d) collided with a previous key", i)
		}
		seen[k] = true
	}
}
