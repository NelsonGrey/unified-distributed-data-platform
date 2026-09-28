package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marknelson/uddp/internal/engine"
)

func testEntries() []engine.SnapshotEntry {
	return []engine.SnapshotEntry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2"), ExpiresAtUnixNano: 1234567890},
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.dat")
	entries := testEntries()

	written, err := WriteFile(path, Meta{NamespaceID: "default", RecoveryPoint: 42}, entries)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if written.EntryCount != 2 || written.SHA256Entries == "" {
		t.Fatalf("expected computed metadata, got %+v", written)
	}

	meta, got, err := ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if meta.NamespaceID != "default" || meta.RecoveryPoint != 42 || meta.EntryCount != 2 {
		t.Fatalf("unexpected meta: %+v", meta)
	}
	if len(got) != 2 || string(got[0].Key) != "a" || string(got[1].Key) != "b" {
		t.Fatalf("unexpected entries: %+v", got)
	}
}

func TestReadRejectsCorruptedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.dat")
	if _, err := WriteFile(path, Meta{NamespaceID: "default"}, testEntries()); err != nil {
		t.Fatalf("write: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte somewhere in the middle to corrupt the payload without
	// breaking gob's own framing (which would just fail to decode at all
	// — flipping near the end targets the entry data more reliably).
	data[len(data)-5] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := ReadFile(path); err == nil {
		t.Fatal("expected corrupted backup to be rejected")
	}
}

func TestWriteFileStampsCurrentVersionRegardlessOfCallerInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.dat")
	if _, err := WriteFile(path, Meta{NamespaceID: "default", FormatVersion: 99}, testEntries()); err != nil {
		t.Fatalf("write: %v", err)
	}
	meta, _, err := ReadFile(path)
	if err != nil {
		t.Fatalf("expected the write-stamped current version to read back fine: %v", err)
	}
	if meta.FormatVersion != currentFormatVersion {
		t.Fatalf("expected WriteFile to stamp the current version, got %d", meta.FormatVersion)
	}
}

func TestReadRejectsUnsupportedVersion(t *testing.T) {
	var buf bytes.Buffer
	entries := testEntries()
	var entryBuf bytes.Buffer
	if err := gob.NewEncoder(&entryBuf).Encode(entries); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(entryBuf.Bytes())

	// Build a payload directly, bypassing Write's version stamping, to
	// simulate a backup file from a future/unknown format version.
	p := payload{
		Meta: Meta{
			FormatVersion: currentFormatVersion + 1,
			NamespaceID:   "default",
			EntryCount:    len(entries),
			SHA256Entries: fmt.Sprintf("%x", sum),
		},
		Entries: entries,
	}
	if err := gob.NewEncoder(&buf).Encode(p); err != nil {
		t.Fatal(err)
	}

	if _, _, err := Read(&buf); err == nil {
		t.Fatal("expected Read to reject an unsupported format version")
	}
}
