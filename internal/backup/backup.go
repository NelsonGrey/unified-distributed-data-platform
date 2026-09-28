// Package backup implements the BR-008/TR-006 backup file format: a
// checksummed, versioned dump of an engine's live state, with enough
// metadata to identify what recovery point it represents.
//
// Explicitly not covered by this slice (documented, not silently missing):
// encryption (TR-006 asks for it; there's no key management in this build
// yet to encrypt with), remote/object-storage targets (local filesystem
// path only), and incremental backups (every backup is a full snapshot).
package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/marknelson/uddp/internal/engine"
)

const currentFormatVersion = 1

// Meta describes one backup — its recovery point and when it was taken.
// TR-006 asks for backups to be "catalogued" and "tied to an evidenced
// recovery point"; this is that evidence, though there's no catalogue/
// index across multiple backups yet (each backup file stands alone).
type Meta struct {
	FormatVersion int
	NamespaceID   string
	RecoveryPoint uint64 // the engine's LastCommitPosition at backup time
	EntryCount    int
	CreatedAt     time.Time
	SHA256Entries string // hex-encoded checksum of the gob-encoded entries, for integrity verification on restore
}

type payload struct {
	Meta    Meta
	Entries []engine.SnapshotEntry
}

// Write serializes meta and entries to w. FormatVersion, EntryCount,
// SHA256Entries, and CreatedAt are computed/set here (any value the caller
// passed in is overwritten), so callers only need to supply NamespaceID
// and RecoveryPoint. It returns the Meta actually written, including
// those computed fields.
func Write(w io.Writer, meta Meta, entries []engine.SnapshotEntry) (Meta, error) {
	var entryBuf bytes.Buffer
	if err := gob.NewEncoder(&entryBuf).Encode(entries); err != nil {
		return Meta{}, fmt.Errorf("backup: encode entries: %w", err)
	}
	sum := sha256.Sum256(entryBuf.Bytes())

	meta.FormatVersion = currentFormatVersion
	meta.EntryCount = len(entries)
	meta.SHA256Entries = fmt.Sprintf("%x", sum)
	meta.CreatedAt = time.Now().UTC()

	if err := gob.NewEncoder(w).Encode(payload{Meta: meta, Entries: entries}); err != nil {
		return Meta{}, fmt.Errorf("backup: encode payload: %w", err)
	}
	return meta, nil
}

// WriteFile is Write against a newly created file at path, fsynced before
// return so a caller can treat a successful WriteFile as durable.
func WriteFile(path string, meta Meta, entries []engine.SnapshotEntry) (Meta, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return Meta{}, fmt.Errorf("backup: create %s: %w", path, err)
	}
	defer f.Close()

	written, err := Write(f, meta, entries)
	if err != nil {
		return Meta{}, err
	}
	if err := f.Sync(); err != nil {
		return Meta{}, fmt.Errorf("backup: sync %s: %w", path, err)
	}
	return written, nil
}

// Read validates the checksum and format version and returns the backup's
// metadata and entries. A checksum mismatch or version mismatch is a hard
// error — TR-005's "recovery shall validate checksums... before serving"
// applies to restore, not just crash recovery.
func Read(r io.Reader) (Meta, []engine.SnapshotEntry, error) {
	var p payload
	if err := gob.NewDecoder(r).Decode(&p); err != nil {
		return Meta{}, nil, fmt.Errorf("backup: decode: %w", err)
	}
	if p.Meta.FormatVersion != currentFormatVersion {
		return Meta{}, nil, fmt.Errorf("backup: unsupported format version %d (this build supports version %d only)", p.Meta.FormatVersion, currentFormatVersion)
	}

	var entryBuf bytes.Buffer
	if err := gob.NewEncoder(&entryBuf).Encode(p.Entries); err != nil {
		return Meta{}, nil, fmt.Errorf("backup: re-encode entries for checksum: %w", err)
	}
	sum := sha256.Sum256(entryBuf.Bytes())
	got := fmt.Sprintf("%x", sum)
	if got != p.Meta.SHA256Entries {
		return Meta{}, nil, fmt.Errorf("backup: checksum mismatch (got %s, want %s) — file is corrupt or was tampered with", got, p.Meta.SHA256Entries)
	}

	return p.Meta, p.Entries, nil
}

// ReadFile is Read against a file at path.
func ReadFile(path string) (Meta, []engine.SnapshotEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return Meta{}, nil, fmt.Errorf("backup: open %s: %w", path, err)
	}
	defer f.Close()
	return Read(f)
}
