// Package wal implements a single-segment, checksummed, append-only write
// log used as the durability substrate for delivery slice 1 (deterministic
// single-node engine). Multi-segment rotation, compaction, and manifest
// lineage (TRD 4.4) are out of scope for this slice.
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// RecordKind distinguishes mutation types recorded in the log.
type RecordKind uint8

const (
	KindPut RecordKind = iota + 1
	KindDelete
)

// Record is one committed mutation. CommitPosition is monotonic and
// assigned by the WAL, matching TRD "commit position" and DDD Partition
// aggregate invariants.
type Record struct {
	CommitPosition    uint64
	Kind              RecordKind
	Key               []byte
	Value             []byte
	ExpiresAtUnixNano int64 // 0 = no expiry
	IdempotencyKey    string
}

// WAL is a checksummed, append-only, crash-recoverable log. It is safe for
// concurrent use, and batches concurrent Append calls into a single
// fsync ("group commit" — see Append) rather than serializing one fsync
// per call, which is the dominant cost of every write (~ms, vs ~µs for
// everything else Append does).
type WAL struct {
	mu   sync.Mutex
	cond *sync.Cond
	file *os.File
	w    *bufio.Writer
	next uint64 // next commit position to assign

	// byteOffsets[i] is the file byte offset of the record with commit
	// position i+1. It backs ReadRange's random access into the log, which
	// is how the change stream (TRD 4.5/§4.5 "log offset") is served to
	// consumers without rescanning from the start on every fetch.
	byteOffsets []int64

	// Group commit bookkeeping. pendingSeq counts every record buffered so
	// far (assigned while holding mu, so it's a total order); flushedSeq is
	// the highest pendingSeq value covered by a completed Flush+Sync. A
	// caller is durably committed once flushedSeq >= the seq it was
	// assigned, regardless of whether it or someone else performed the
	// actual fsync.
	pendingSeq  uint64
	flushedSeq  uint64
	flushErr    error
	leading     bool
	writeOffset int64 // logical end of the log, including buffered-but-not-yet-flushed bytes
}

// Open opens or creates the log at path and replays it, invoking replay for
// every valid record found in file order. Replay stops at the first
// incomplete or corrupt trailing record, per TR-005 ("recovery shall
// validate checksums ... before serving"); a mid-file checksum failure is a
// hard error since it indicates corruption rather than a torn tail write.
func Open(path string, replay func(Record) error) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	next, byteOffsets, err := recover_(f, replay)
	if err != nil {
		f.Close()
		return nil, err
	}

	endOffset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: seek end: %w", err)
	}

	w := &WAL{file: f, w: bufio.NewWriter(f), next: next, byteOffsets: byteOffsets, writeOffset: endOffset}
	w.cond = sync.NewCond(&w.mu)
	return w, nil
}

// recover_ scans the log from the start, validating each record's checksum
// and enforcing monotonic commit positions. It returns the next commit
// position to assign and the byte offset of every valid record found.
func recover_(f *os.File, replay func(Record) error) (uint64, []int64, error) {
	r := bufio.NewReader(f)
	var next uint64 = 1
	var offset int64
	var byteOffsets []int64

	for {
		rec, n, err := readRecord(r)
		if err == io.EOF {
			break
		}
		if err == errTornRecord {
			// Truncate the incomplete trailing write so future appends
			// start from a clean, validated boundary.
			if terr := f.Truncate(offset); terr != nil {
				return 0, nil, fmt.Errorf("wal: truncate torn tail: %w", terr)
			}
			break
		}
		if err != nil {
			return 0, nil, fmt.Errorf("wal: corrupt record at offset %d: %w", offset, err)
		}
		if rec.CommitPosition != next {
			return 0, nil, fmt.Errorf("wal: non-monotonic commit position at offset %d: got %d want %d", offset, rec.CommitPosition, next)
		}
		if replay != nil {
			if err := replay(*rec); err != nil {
				return 0, nil, fmt.Errorf("wal: replay commit %d: %w", rec.CommitPosition, err)
			}
		}
		byteOffsets = append(byteOffsets, offset)
		offset += n
		next++
	}

	return next, byteOffsets, nil
}

var errTornRecord = fmt.Errorf("torn record")

// record wire format (little-endian):
//
//	uint32 length      (bytes following, excluding this field)
//	uint32 crc32        (over kind..idempotencyKey)
//	uint64 commitPosition
//	uint8  kind
//	uint32 keyLen        + key
//	uint32 valueLen      + value
//	int64  expiresAtUnixNano
//	uint32 idempotencyKeyLen + idempotencyKey
func readRecord(r io.Reader) (*Record, int64, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if err == io.EOF {
			return nil, 0, io.EOF
		}
		return nil, 0, errTornRecord
	}
	length := binary.LittleEndian.Uint32(lenBuf[:])

	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, 0, errTornRecord
	}

	if len(body) < 4 {
		return nil, 0, errTornRecord
	}
	wantCRC := binary.LittleEndian.Uint32(body[:4])
	payload := body[4:]
	gotCRC := crc32.ChecksumIEEE(payload)
	if gotCRC != wantCRC {
		return nil, 0, fmt.Errorf("checksum mismatch: got %x want %x", gotCRC, wantCRC)
	}

	rec, err := decodePayload(payload)
	if err != nil {
		return nil, 0, err
	}
	return rec, int64(4 + length), nil
}

func decodePayload(p []byte) (*Record, error) {
	if len(p) < 8+1+4 {
		return nil, fmt.Errorf("payload too short")
	}
	rec := &Record{}
	rec.CommitPosition = binary.LittleEndian.Uint64(p[0:8])
	rec.Kind = RecordKind(p[8])
	off := 9

	keyLen := int(binary.LittleEndian.Uint32(p[off : off+4]))
	off += 4
	if off+keyLen > len(p) {
		return nil, fmt.Errorf("truncated key")
	}
	rec.Key = append([]byte(nil), p[off:off+keyLen]...)
	off += keyLen

	if off+4 > len(p) {
		return nil, fmt.Errorf("truncated value length")
	}
	valLen := int(binary.LittleEndian.Uint32(p[off : off+4]))
	off += 4
	if off+valLen > len(p) {
		return nil, fmt.Errorf("truncated value")
	}
	rec.Value = append([]byte(nil), p[off:off+valLen]...)
	off += valLen

	if off+8 > len(p) {
		return nil, fmt.Errorf("truncated expiry")
	}
	rec.ExpiresAtUnixNano = int64(binary.LittleEndian.Uint64(p[off : off+8]))
	off += 8

	if off+4 > len(p) {
		return nil, fmt.Errorf("truncated idempotency key length")
	}
	idLen := int(binary.LittleEndian.Uint32(p[off : off+4]))
	off += 4
	if off+idLen > len(p) {
		return nil, fmt.Errorf("truncated idempotency key")
	}
	rec.IdempotencyKey = string(p[off : off+idLen])

	return rec, nil
}

func encodePayload(rec Record) []byte {
	idLen := len(rec.IdempotencyKey)
	payload := make([]byte, 8+1+4+len(rec.Key)+4+len(rec.Value)+8+4+idLen)
	off := 0
	binary.LittleEndian.PutUint64(payload[off:], rec.CommitPosition)
	off += 8
	payload[off] = byte(rec.Kind)
	off++
	binary.LittleEndian.PutUint32(payload[off:], uint32(len(rec.Key)))
	off += 4
	off += copy(payload[off:], rec.Key)
	binary.LittleEndian.PutUint32(payload[off:], uint32(len(rec.Value)))
	off += 4
	off += copy(payload[off:], rec.Value)
	binary.LittleEndian.PutUint64(payload[off:], uint64(rec.ExpiresAtUnixNano))
	off += 8
	binary.LittleEndian.PutUint32(payload[off:], uint32(idLen))
	off += 4
	copy(payload[off:], rec.IdempotencyKey)
	return payload
}

// Append assigns the next commit position, durably commits the record
// (fsync before returning), and returns the assigned position. Concurrent
// callers still see per-partition serialized commit ordering (TRD 4.3):
// positions are assigned strictly in arrival order under mu. But Append
// does not give every caller its own fsync — the first caller to find no
// fsync already in flight becomes the "leader" for a batch, and every
// other concurrent caller "rides along" on that fsync (or the next one, if
// they arrive after the leader has already started it) instead of issuing
// their own. This is the standard WAL group-commit pattern: fsync latency
// (milliseconds) dominates everything else Append does (microseconds), so
// batching it is the highest-leverage single change for concurrent
// throughput. A caller never returns before its own bytes are actually
// durable — see the flushedSeq bookkeeping below for why that holds even
// for a follower whose write lands mid-flight.
func (w *WAL) Append(rec Record) (uint64, error) {
	w.mu.Lock()

	rec.CommitPosition = w.next
	w.next++
	payload := encodePayload(rec)
	crc := crc32.ChecksumIEEE(payload)

	body := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(body[:4], crc)
	copy(body[4:], payload)

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(body)))

	recordOffset := w.writeOffset
	if _, err := w.w.Write(lenBuf[:]); err != nil {
		w.mu.Unlock()
		return 0, fmt.Errorf("wal: write length: %w", err)
	}
	if _, err := w.w.Write(body); err != nil {
		w.mu.Unlock()
		return 0, fmt.Errorf("wal: write body: %w", err)
	}
	w.writeOffset += int64(len(lenBuf)) + int64(len(body))
	w.byteOffsets = append(w.byteOffsets, recordOffset)

	w.pendingSeq++
	mySeq := w.pendingSeq

	if w.leading {
		// A flush covering at least up to this point is already guaranteed
		// (see the leader loop below), so just wait for it.
		for w.flushedSeq < mySeq {
			w.cond.Wait()
		}
		err := w.flushErr
		w.mu.Unlock()
		if err != nil {
			return 0, err
		}
		return rec.CommitPosition, nil
	}

	w.leading = true
	for {
		flushSeq := w.pendingSeq // everything buffered up to here will be covered by this round
		if err := w.w.Flush(); err != nil {
			w.leading = false
			w.flushErr = fmt.Errorf("wal: flush: %w", err)
			// Every current waiter is stuck behind this flush and has no
			// way to know more precisely what got written, so unblock them
			// all with the error rather than leave them waiting on a
			// flushedSeq that will never advance.
			w.flushedSeq = w.pendingSeq
			w.cond.Broadcast()
			w.mu.Unlock()
			return 0, w.flushErr
		}

		w.mu.Unlock()
		syncErr := w.file.Sync()
		w.mu.Lock()

		if syncErr != nil {
			w.flushErr = fmt.Errorf("wal: fsync: %w", syncErr)
			// Same reasoning as the Flush()-error path above: unblock
			// every current waiter, not just the ones this round's
			// snapshot covered, or anyone who arrived while we were
			// inside Sync() (mySeq > flushSeq) would wait forever with no
			// one left leading to cover them.
			w.flushedSeq = w.pendingSeq
			w.leading = false
			w.cond.Broadcast()
			err := w.flushErr
			w.mu.Unlock()
			return 0, err
		}

		w.flushedSeq = flushSeq
		w.flushErr = nil
		w.cond.Broadcast()

		if w.pendingSeq == flushSeq {
			// Nothing new arrived while we were syncing; step down.
			w.leading = false
			w.mu.Unlock()
			return rec.CommitPosition, nil
		}
		// More records were buffered while we were syncing (by followers
		// who arrived mid-flight, or racing new leaders — but leading is
		// still true so they queued as followers). Flush again to cover
		// them before stepping down, rather than leaving a follower
		// waiting for a leader that never shows up.
	}
}

// ReadRange returns up to limit records starting at commit position from
// (inclusive), in commit order. It returns fewer than limit records (or
// none) if the log doesn't yet have that many past from. Concurrent with
// Append: it only reads byte ranges already fsynced, via a read-only handle
// so it never interferes with the writer's file offset.
func (w *WAL) ReadRange(from uint64, limit int) ([]Record, error) {
	w.mu.Lock()
	if from < 1 || int(from-1) >= len(w.byteOffsets) || limit <= 0 {
		w.mu.Unlock()
		return nil, nil
	}
	startOffset := w.byteOffsets[from-1]
	w.mu.Unlock()

	fi, err := w.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("wal: stat: %w", err)
	}
	sr := io.NewSectionReader(w.file, startOffset, fi.Size()-startOffset)
	r := bufio.NewReader(sr)

	records := make([]Record, 0, limit)
	for i := 0; i < limit; i++ {
		rec, _, err := readRecord(r)
		if err == io.EOF || err == errTornRecord {
			break
		}
		if err != nil {
			return records, fmt.Errorf("wal: read range at position %d: %w", from+uint64(i), err)
		}
		records = append(records, *rec)
	}
	return records, nil
}

// Close flushes and closes the underlying file. It waits for any in-flight
// group-commit fsync to finish first — that fsync runs without mu held
// (see Append), so closing the file out from under it would be a real bug,
// not just a theoretical one.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.leading {
		w.cond.Wait()
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}
