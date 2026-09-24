package catalog

import (
	"errors"
	"testing"
)

func TestNewNamespaceSpecRejectsUnsupportedProfile(t *testing.T) {
	if _, err := NewNamespaceSpec("ns1", "durable"); !errors.Is(err, ErrUnsupportedProfile) {
		t.Fatalf("expected ErrUnsupportedProfile for durable, got %v", err)
	}
	if _, err := NewNamespaceSpec("ns1", "strong"); !errors.Is(err, ErrUnsupportedProfile) {
		t.Fatalf("expected ErrUnsupportedProfile for strong, got %v", err)
	}
	if _, err := NewNamespaceSpec("ns1", "cache"); err != nil {
		t.Fatalf("expected cache profile to be accepted, got %v", err)
	}
}

func TestNewNamespaceSpecRejectsEmptyID(t *testing.T) {
	if _, err := NewNamespaceSpec("", "cache"); err == nil {
		t.Fatal("expected error for empty namespace id")
	}
}

func TestRegistryValidate(t *testing.T) {
	spec, err := NewNamespaceSpec("default", "cache")
	if err != nil {
		t.Fatalf("new spec: %v", err)
	}
	reg := NewRegistry(spec)

	if err := reg.Validate("default"); err != nil {
		t.Fatalf("expected the configured namespace to validate, got %v", err)
	}
	if err := reg.Validate("other"); !errors.Is(err, ErrNamespaceNotFound) {
		t.Fatalf("expected ErrNamespaceNotFound for unknown namespace, got %v", err)
	}
}
