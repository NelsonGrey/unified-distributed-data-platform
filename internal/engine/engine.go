// Package engine implements the deterministic single-node partition state
// machine for delivery slice 1: a memory index backed by the WAL, with
// crash recovery, TTL expiry, conditional writes, and idempotent retries
// (TR-002, TR-003, TR-005).
package engine

import (
	"errors"
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
// before making the mutation visible in the index (TRD 4.5).
func (e *Engine) Put(key, value []byte, ttlSeconds int64, idempotencyKey string) (Outcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if idempotencyKey != "" {
		if out, ok := e.dedup[idempotencyKey]; ok {
			out.Deduplicated = true
			return out, nil
		}
	}

	var expiresAt time.Time
	var expiresAtNano int64
	if ttlSeconds > 0 {
		expiresAt = e.clock.Now().Add(time.Duration(ttlSeconds) * time.Second)
		expiresAtNano = expiresAt.UnixNano()
	}

	pos, err := e.log.Append(wal.Record{
		Kind:              wal.KindPut,
		Key:               key,
		Value:             value,
		ExpiresAtUnixNano: expiresAtNano,
		IdempotencyKey:    idempotencyKey,
	})
	if err != nil {
		return Outcome{}, err
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
// ErrVersionMismatch on conflict without committing anything.
func (e *Engine) CompareAndSet(key, value []byte, expectedVersion uint64, ttlSeconds int64, idempotencyKey string) (Outcome, error) {
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

	var expiresAt time.Time
	var expiresAtNano int64
	if ttlSeconds > 0 {
		expiresAt = e.clock.Now().Add(time.Duration(ttlSeconds) * time.Second)
		expiresAtNano = expiresAt.UnixNano()
	}

	pos, err := e.log.Append(wal.Record{
		Kind:              wal.KindPut,
		Key:               key,
		Value:             value,
		ExpiresAtUnixNano: expiresAtNano,
		IdempotencyKey:    idempotencyKey,
	})
	if err != nil {
		return Outcome{}, err
	}

	e.index[string(key)] = &entry{value: value, version: pos, expiresAt: expiresAt}

	out := Outcome{CommitPosition: pos, Version: pos}
	if idempotencyKey != "" {
		e.dedup[idempotencyKey] = out
	}
	return out, nil
}

// Close releases the underlying WAL file.
func (e *Engine) Close() error {
	return e.log.Close()
}
