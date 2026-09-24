// Package catalog implements the minimal slice of the DDD "Resource
// Catalog" bounded context this build needs: validating that a namespace's
// declared workload profile is one this node can actually honor, and
// rejecting requests addressed to a namespace this node doesn't serve.
//
// A single uddp-node process is single-partition in this delivery slice,
// so the "registry" is one validated NamespaceSpec rather than a
// multi-tenant catalog — that's a later-slice concern (DDD: Resource
// Catalog "is the source of desired configuration, not runtime truth").
package catalog

import (
	"errors"
	"fmt"
)

// ErrUnsupportedProfile is returned when a namespace declares a workload
// profile this build cannot honor.
var ErrUnsupportedProfile = errors.New("catalog: unsupported workload profile")

// ErrNamespaceNotFound is returned by Registry.Validate when a request
// names a namespace this node doesn't serve.
var ErrNamespaceNotFound = errors.New("catalog: namespace not found")

// alwaysSupportedProfiles lists profiles honest to serve regardless of
// topology. "strong" requires consensus semantics beyond plain quorum
// replication (linearizable conditional state — TRD 4.3) that aren't
// implemented yet, so it stays gated even with replication enabled.
var alwaysSupportedProfiles = map[string]bool{
	"cache": true,
}

// NamespaceSpec is the namespace's policy/API boundary declaration (DDD
// Namespace aggregate root), reduced to what this slice enforces: an ID
// and its one immutable WorkloadProfileVersion.
type NamespaceSpec struct {
	ID      string
	Profile string
}

// NewNamespaceSpec validates id and profile and returns the resulting
// spec. It fails fast at startup rather than accepting a namespace this
// node can't actually back. replicationEnabled gates the "durable"
// profile: it requires WAL durability plus replica quorum (TRD 4.3), so
// declaring it on a node with no replica group would misrepresent the
// durability contract (BR-002) — this check is what stops that.
func NewNamespaceSpec(id, profile string, replicationEnabled bool) (NamespaceSpec, error) {
	if id == "" {
		return NamespaceSpec{}, fmt.Errorf("catalog: namespace id must not be empty")
	}
	supported := alwaysSupportedProfiles[profile] || (profile == "durable" && replicationEnabled)
	if !supported {
		want := "cache"
		if !replicationEnabled {
			want = "cache (durable requires replication to be enabled on this node)"
		} else {
			want = "cache, durable"
		}
		return NamespaceSpec{}, fmt.Errorf("%w: %q (supported: %s)", ErrUnsupportedProfile, profile, want)
	}
	return NamespaceSpec{ID: id, Profile: profile}, nil
}

// Registry resolves namespace IDs to their spec. In this slice it always
// holds exactly one namespace — the one this node was started with.
type Registry struct {
	spec NamespaceSpec
}

// NewRegistry returns a Registry serving exactly spec.
func NewRegistry(spec NamespaceSpec) *Registry {
	return &Registry{spec: spec}
}

// Validate returns nil if id is the namespace this node serves, or
// ErrNamespaceNotFound otherwise. Callers should surface this as an
// explicit error to the client (TR-010) rather than silently operating
// against whatever namespace_id was sent.
func (r *Registry) Validate(id string) error {
	if id != r.spec.ID {
		return fmt.Errorf("%w: %q (this node serves %q)", ErrNamespaceNotFound, id, r.spec.ID)
	}
	return nil
}

// Spec returns the namespace this registry serves.
func (r *Registry) Spec() NamespaceSpec {
	return r.spec
}
