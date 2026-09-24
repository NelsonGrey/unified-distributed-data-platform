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
// concurrent use.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	w    *bufio.Writer
	next uint64 // next commit position to assign
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

	next, err := recover_(f, replay)
	if err != nil {
		f.Close()
		return nil, err
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: seek end: %w", err)
	}

	return &WAL{file: f, w: bufio.NewWriter(f), next: next}, nil
}

// recover_ scans the log from the start, validating each record's checksum
// and enforcing monotonic commit positions. It returns the next commit
// position to assign.
func recover_(f *os.File, replay func(Record) error) (uint64, error) {
	r := bufio.NewReader(f)
	var next uint64 = 1
	var offset int64

	for {
		rec, n, err := readRecord(r)
		if err == io.EOF {
			break
		}
		if err == errTornRecord {
			// Truncate the incomplete trailing write so future appends
			// start from a clean, validated boundary.
			if terr := f.Truncate(offset); terr != nil {
				return 0, fmt.Errorf("wal: truncate torn tail: %w", terr)
			}
			break
		}
		if err != nil {
			return 0, fmt.Errorf("wal: corrupt record at offset %d: %w", offset, err)
		}
		if rec.CommitPosition != next {
			return 0, fmt.Errorf("wal: non-monotonic commit position at offset %d: got %d want %d", offset, rec.CommitPosition, next)
		}
		if replay != nil {
			if err := replay(*rec); err != nil {
				return 0, fmt.Errorf("wal: replay commit %d: %w", rec.CommitPosition, err)
			}
		}
		offset += n
		next++
	}

	return next, nil
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
func readRecord(r *bufio.Reader) (*Record, int64, error) {
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

// Append assigns the next commit position, writes the record durably
// (fsync before returning), and returns the assigned position. Append
// serializes concurrent callers to preserve the "per-partition serialized
// commit ordering" principle from TRD 4.3.
func (w *WAL) Append(rec Record) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec.CommitPosition = w.next
	payload := encodePayload(rec)
	crc := crc32.ChecksumIEEE(payload)

	body := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(body[:4], crc)
	copy(body[4:], payload)

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(body)))

	if _, err := w.w.Write(lenBuf[:]); err != nil {
		return 0, fmt.Errorf("wal: write length: %w", err)
	}
	if _, err := w.w.Write(body); err != nil {
		return 0, fmt.Errorf("wal: write body: %w", err)
	}
	if err := w.w.Flush(); err != nil {
		return 0, fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("wal: fsync: %w", err)
	}

	w.next++
	return rec.CommitPosition, nil
}

// Close flushes and closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}
