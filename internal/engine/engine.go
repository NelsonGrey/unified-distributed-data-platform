// Package engine implements the deterministic single-node partition state
// machine for delivery slice 1: a memory index backed by the WAL, with
// crash recovery, TTL expiry, conditional writes, and idempotent retries
// (TR-002, TR-003, TR-005).
package engine

import (
	"errors"
	"os"
	"sync"
	"time"

	"github.com/marknelson/uddp/internal/wal"
)

var (
	// ErrVersionMismatch is returned by CompareAndSet when the expected
	// version does not match the current stored version.
	ErrVersionMismatch = errors.New("engine: version mismatch")
	// ErrNotFound is returned when an operation requires an existing key.
	ErrNotFound = errors.New("engine: key not found")
)

type entry struct {
	value     []byte
	version   uint64
	expiresAt time.Time // zero = no expiry
}

// Outcome describes the result of a mutation, mirroring MutationResponse
// fields the API layer needs (commit position, version, dedup flag).
type Outcome struct {
	CommitPosition uint64
	Version        uint64
	Deduplicated   bool
}

// Clock abstracts time for deterministic tests (TRD 2: "deterministic
// state machines and injectable clocks").
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Engine is a single-partition, single-node key-value state machine.
// Namespace/partition/epoch identity is attached by the caller (API layer)
// since this slice runs exactly one partition.
type Engine struct {
	mu    sync.RWMutex
	log   *wal.WAL
	index map[string]*entry

	// dedup remembers the outcome of the last commit for a given
	// idempotency key, bounded to the process lifetime in this slice.
	// Retention-window-bounded dedup persisted across restarts is a
	// later-slice concern (TR-003 notes "within the retention window").
	dedup map[string]Outcome

	clock Clock
}

// Open recovers the engine's index from the WAL at path and returns a
// ready Engine.
func Open(path string, clock Clock) (*Engine, error) {
	if clock == nil {
		clock = systemClock{}
	}
	e := &Engine{
		index: make(map[string]*entry),
		dedup: make(map[string]Outcome),
		clock: clock,
	}

	log, err := wal.Open(path, e.replay)
	if err != nil {
		return nil, err
	}
	e.log = log
	return e, nil
}

func (e *Engine) replay(rec wal.Record) error {
	key := string(rec.Key)
	switch rec.Kind {
	case wal.KindPut:
		ent := &entry{value: rec.Value, version: rec.CommitPosition}
		if rec.ExpiresAtUnixNano > 0 {
			ent.expiresAt = time.Unix(0, rec.ExpiresAtUnixNano)
		}
		e.index[key] = ent
	case wal.KindDelete:
		delete(e.index, key)
	}
	if rec.IdempotencyKey != "" {
		e.dedup[rec.IdempotencyKey] = Outcome{CommitPosition: rec.CommitPosition, Version: rec.CommitPosition}
	}
	return nil
}

// Get returns the current value for key. found is false if the key is
// absent or has expired.
func (e *Engine) Get(key []byte) (value []byte, version uint64, found bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	ent, ok := e.index[string(key)]
	if !ok {
		return nil, 0, false
	}
	if e.expired(ent) {
		return nil, 0, false
	}
	return ent.value, ent.version, true
}

func (e *Engine) expired(ent *entry) bool {
	return !ent.expiresAt.IsZero() && e.clock.Now().After(ent.expiresAt)
}

// Put unconditionally writes key/value, committing atomically to the WAL
// before making the mutation visible in the index (TRD 4.5). ttlSeconds is
// resolved against this engine's own clock — correct for the single-node,
// directly-driven path, but NOT safe to call independently on multiple
// replicas of the same logical write (see ApplyPut).
func (e *Engine) Put(key, value []byte, ttlSeconds int64, idempotencyKey string) (Outcome, error) {
	var expiresAtNano int64
	if ttlSeconds > 0 {
		expiresAtNano = e.clock.Now().Add(time.Duration(ttlSeconds) * time.Second).UnixNano()
	}
	return e.ApplyPut(key, value, expiresAtNano, idempotencyKey)
}

// ApplyPut is Put with an already-resolved absolute expiry (0 = no
// expiry) instead of a relative TTL. It exists so a replicated command can
// carry one expiry timestamp — decided once, by whichever node proposes
// it — that every replica applies identically. If each replica instead
// computed "now + ttlSeconds" independently, replicas would disagree
// (however slightly) on expiry, which is a real determinism violation for
// a replicated state machine (TRD 2: "deterministic state machines"), not
// just a cosmetic one — a lagging replica catching up minutes later would
// compute a materially different expiry than the leader did at proposal
// time.
func (e *Engine) ApplyPut(key, value []byte, expiresAtUnixNano int64, idempotencyKey string) (Outcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if idempotencyKey != "" {
		if out, ok := e.dedup[idempotencyKey]; ok {
			out.Deduplicated = true
			return out, nil
		}
	}

	pos, err := e.log.Append(wal.Record{
		Kind:              wal.KindPut,
		Key:               key,
		Value:             value,
		ExpiresAtUnixNano: expiresAtUnixNano,
		IdempotencyKey:    idempotencyKey,
	})
	if err != nil {
		return Outcome{}, err
	}

	var expiresAt time.Time
	if expiresAtUnixNano > 0 {
		expiresAt = time.Unix(0, expiresAtUnixNano)
	}
	e.index[string(key)] = &entry{value: value, version: pos, expiresAt: expiresAt}

	out := Outcome{CommitPosition: pos, Version: pos}
	if idempotencyKey != "" {
		e.dedup[idempotencyKey] = out
	}
	return out, nil
}

// Delete removes key if present. Deleting an absent key is not an error;
// it still commits a tombstone so replay and downstream change records
// observe a consistent ordered history.
func (e *Engine) Delete(key []byte, idempotencyKey string) (Outcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if idempotencyKey != "" {
		if out, ok := e.dedup[idempotencyKey]; ok {
			out.Deduplicated = true
			return out, nil
		}
	}

	pos, err := e.log.Append(wal.Record{
		Kind:           wal.KindDelete,
		Key:            key,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return Outcome{}, err
	}
	delete(e.index, string(key))

	out := Outcome{CommitPosition: pos, Version: pos}
	if idempotencyKey != "" {
		e.dedup[idempotencyKey] = out
	}
	return out, nil
}

// CompareAndSet writes value only if the current version equals
// expectedVersion (0 meaning "must not exist"). It returns
// ErrVersionMismatch on conflict without committing anything. Like Put,
// ttlSeconds is resolved against this engine's own clock — see ApplyPut
// for why that's not safe across independently-applying replicas.
func (e *Engine) CompareAndSet(key, value []byte, expectedVersion uint64, ttlSeconds int64, idempotencyKey string) (Outcome, error) {
	var expiresAtNano int64
	if ttlSeconds > 0 {
		expiresAtNano = e.clock.Now().Add(time.Duration(ttlSeconds) * time.Second).UnixNano()
	}
	return e.ApplyCompareAndSet(key, value, expectedVersion, expiresAtNano, idempotencyKey)
}

// ApplyCompareAndSet is CompareAndSet with an already-resolved absolute
// expiry; see ApplyPut.
func (e *Engine) ApplyCompareAndSet(key, value []byte, expectedVersion uint64, expiresAtUnixNano int64, idempotencyKey string) (Outcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if idempotencyKey != "" {
		if out, ok := e.dedup[idempotencyKey]; ok {
			out.Deduplicated = true
			return out, nil
		}
	}

	ent, exists := e.index[string(key)]
	if exists && e.expired(ent) {
		exists = false
	}

	var currentVersion uint64
	if exists {
		currentVersion = ent.version
	}
	if currentVersion != expectedVersion {
		return Outcome{}, ErrVersionMismatch
	}

	pos, err := e.log.Append(wal.Record{
		Kind:              wal.KindPut,
		Key:               key,
		Value:             value,
		ExpiresAtUnixNano: expiresAtUnixNano,
		IdempotencyKey:    idempotencyKey,
	})
	if err != nil {
		return Outcome{}, err
	}

	var expiresAt time.Time
	if expiresAtUnixNano > 0 {
		expiresAt = time.Unix(0, expiresAtUnixNano)
	}
	e.index[string(key)] = &entry{value: value, version: pos, expiresAt: expiresAt}

	out := Outcome{CommitPosition: pos, Version: pos}
	if idempotencyKey != "" {
		e.dedup[idempotencyKey] = out
	}
	return out, nil
}

// ChangeRecord is one entry in the partition's ordered change stream. Its
// Offset is the same commit position visible as the mutation's Version
// (TRD 4.5: "Commit makes the state version and log offset visible
// together") — there is no separate change log to fall out of sync with
// state, which is what removes the customer-managed dual-write (BR-003).
type ChangeRecord struct {
	Offset uint64
	Kind   wal.RecordKind
	Key    []byte
	Value  []byte
}

// Fetch returns up to limit change records starting at offset from
// (inclusive), in commit order. Every committed Put/Delete produces exactly
// one change record; there is no per-namespace opt-out in this slice since
// the always-on change stream is the product's core differentiator.
func (e *Engine) Fetch(from uint64, limit int) ([]ChangeRecord, error) {
	recs, err := e.log.ReadRange(from, limit)
	if err != nil {
		return nil, err
	}
	out := make([]ChangeRecord, len(recs))
	for i, r := range recs {
		out[i] = ChangeRecord{Offset: r.CommitPosition, Kind: r.Kind, Key: r.Key, Value: r.Value}
	}
	return out, nil
}

// SnapshotEntry is one live key/value as captured by Snapshot, used by a
// replication layer to bring a lagging or new replica's engine up to date
// without replaying the full command history (see internal/replication).
type SnapshotEntry struct {
	Key               []byte
	Value             []byte
	ExpiresAtUnixNano int64 // 0 = no expiry
}

// Snapshot returns every key not already expired as of now. It does not
// include tombstones (deletes) or dedup state — a key absent from the
// snapshot is simply absent after Reset+reload, which is equivalent to
// having been deleted or never written.
func (e *Engine) Snapshot() []SnapshotEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]SnapshotEntry, 0, len(e.index))
	for k, ent := range e.index {
		if e.expired(ent) {
			continue
		}
		var expiresAtUnixNano int64
		if !ent.expiresAt.IsZero() {
			expiresAtUnixNano = ent.expiresAt.UnixNano()
		}
		out = append(out, SnapshotEntry{Key: []byte(k), Value: ent.value, ExpiresAtUnixNano: expiresAtUnixNano})
	}
	return out
}

// Reset wipes the engine's state and its underlying WAL file, returning it
// to the same state as a brand-new Engine opened at this path. It exists
// for restoring a replica from a snapshot (see internal/replication):
// after Reset, the caller re-applies snapshot entries via ApplyPut to
// rebuild state, this time from the snapshot instead of full log replay.
// Reset is destructive and not part of the normal single-node write path.
func (e *Engine) Reset() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	path := e.log.Path()
	if err := e.log.Close(); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	log, err := wal.Open(path, nil)
	if err != nil {
		return err
	}
	e.log = log
	e.index = make(map[string]*entry)
	e.dedup = make(map[string]Outcome)
	return nil
}

// Close releases the underlying WAL file.
func (e *Engine) Close() error {
	return e.log.Close()
}
