// Package streaming implements the consumer side of the change stream:
// durable per-consumer-group offset commits (DDD "Streaming and
// Consumption" bounded context, ConsumerGroup/OffsetCommit). It does not
// own or alter committed partition history — that stays in
// internal/engine.
package streaming

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/marknelson/uddp/internal/wal"
)

// ErrOffsetRegression is returned by Commit when offset is behind the
// group's currently committed offset.
var ErrOffsetRegression = errors.New("streaming: offset commit would move backward")

// OffsetStore durably tracks the last committed offset per consumer group.
// It is backed by its own WAL so offset commits survive restart and can't
// silently roll back (DDD: "committed offset cannot move backward unless
// an explicit, authorized reset operation is recorded" — this slice has no
// reset operation yet, so commits are monotonic by construction).
type OffsetStore struct {
	mu      sync.RWMutex
	log     *wal.WAL
	offsets map[string]uint64
}

// Open recovers committed offsets from the log at path.
func Open(path string) (*OffsetStore, error) {
	s := &OffsetStore{offsets: make(map[string]uint64)}
	log, err := wal.Open(path, s.replay)
	if err != nil {
		return nil, err
	}
	s.log = log
	return s, nil
}

func (s *OffsetStore) replay(rec wal.Record) error {
	s.offsets[string(rec.Key)] = decodeOffset(rec.Value)
	return nil
}

func encodeOffset(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func decodeOffset(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

// Commit durably records offset as the next fetch position for group. It
// rejects attempts to move the offset backward, since that would silently
// replay already-processed change records to the group.
func (s *OffsetStore) Commit(group string, offset uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if current, ok := s.offsets[group]; ok && offset < current {
		return current, ErrOffsetRegression
	}

	if _, err := s.log.Append(wal.Record{Kind: wal.KindPut, Key: []byte(group), Value: encodeOffset(offset)}); err != nil {
		return 0, err
	}
	s.offsets[group] = offset
	return offset, nil
}

// Fetch returns the last committed offset for group, if any.
func (s *OffsetStore) Fetch(group string) (offset uint64, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	offset, found = s.offsets[group]
	return offset, found
}

// Close releases the underlying log.
func (s *OffsetStore) Close() error {
	return s.log.Close()
}
