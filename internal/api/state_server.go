// Package api adapts the internal engine to the native gRPC StateService
// (api/native/v1). It is the "API edge" component from TRD 4.1, scoped to
// the single-node, single-partition slice: namespace/quota/auth enforcement
// beyond a fixed partition/epoch are later-slice concerns.
package api

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nativev1 "github.com/marknelson/uddp/api/native/v1"
	"github.com/marknelson/uddp/internal/catalog"
	"github.com/marknelson/uddp/internal/engine"
	"github.com/marknelson/uddp/internal/observability"
	"github.com/marknelson/uddp/internal/replication"
)

// StateServer implements nativev1.StateServiceServer over a single local
// engine instance representing partition 0, epoch 0.
type StateServer struct {
	nativev1.UnimplementedStateServiceServer

	Engine   *engine.Engine
	Registry *catalog.Registry
	Metrics  *observability.Metrics // optional; nil disables commit-position gauge updates

	// Replication is optional. When set, mutations are proposed through
	// raft instead of applied directly to Engine — Engine is then driven
	// exclusively by replication.FSM.Apply (see cmd/uddp-node), and this
	// node's response epoch reflects the raft term the write committed
	// under. When nil, this is the single-node slice-1 behavior: mutations
	// apply directly, "durable"/"strong" aren't honestly claimable (TRD
	// 4.3 requires replica quorum for them).
	Replication *replication.Node
}

func (s *StateServer) Get(_ context.Context, req *nativev1.GetRequest) (*nativev1.GetResponse, error) {
	if err := checkNamespace(s.Registry, req.NamespaceId); err != nil {
		return nil, err
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	// Reads are served from this node's local state regardless of
	// replication mode. That's linearizable only on the leader immediately
	// after a quorum commit; a follower (or a leader mid-election) can
	// serve a stale read. Raft's read-index/lease-read mechanisms would
	// close that gap but aren't implemented in this slice — documented,
	// not silently claimed away, consistent with `strong` staying gated.
	value, version, found := s.Engine.Get(req.Key)
	return &nativev1.GetResponse{Value: value, Version: version, Found: found}, nil
}

func (s *StateServer) Put(_ context.Context, req *nativev1.PutRequest) (*nativev1.MutationResponse, error) {
	if err := checkNamespace(s.Registry, req.NamespaceId); err != nil {
		return nil, err
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if s.Replication != nil {
		result, err := s.Replication.Propose(replication.Command{
			Op:                replication.OpPut,
			Key:               req.Key,
			Value:             req.Value,
			ExpiresAtUnixNano: resolveExpiry(req.TtlSeconds),
			IdempotencyKey:    req.IdempotencyKey,
		})
		return s.replicatedResponse(result, err, "put")
	}

	out, err := s.Engine.Put(req.Key, req.Value, req.TtlSeconds, req.IdempotencyKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	return s.response(out, 0), nil
}

func (s *StateServer) Delete(_ context.Context, req *nativev1.DeleteRequest) (*nativev1.MutationResponse, error) {
	if err := checkNamespace(s.Registry, req.NamespaceId); err != nil {
		return nil, err
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if s.Replication != nil {
		result, err := s.Replication.Propose(replication.Command{
			Op:             replication.OpDelete,
			Key:            req.Key,
			IdempotencyKey: req.IdempotencyKey,
		})
		return s.replicatedResponse(result, err, "delete")
	}

	out, err := s.Engine.Delete(req.Key, req.IdempotencyKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return s.response(out, 0), nil
}

func (s *StateServer) CompareAndSet(_ context.Context, req *nativev1.CompareAndSetRequest) (*nativev1.MutationResponse, error) {
	if err := checkNamespace(s.Registry, req.NamespaceId); err != nil {
		return nil, err
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if s.Replication != nil {
		result, err := s.Replication.Propose(replication.Command{
			Op:                replication.OpCompareAndSet,
			Key:               req.Key,
			Value:             req.Value,
			ExpiresAtUnixNano: resolveExpiry(req.TtlSeconds),
			ExpectedVersion:   req.ExpectedVersion,
			IdempotencyKey:    req.IdempotencyKey,
		})
		return s.replicatedResponse(result, err, "compare_and_set")
	}

	out, err := s.Engine.CompareAndSet(req.Key, req.Value, req.ExpectedVersion, req.TtlSeconds, req.IdempotencyKey)
	if err != nil {
		if errors.Is(err, engine.ErrVersionMismatch) {
			return nil, status.Error(codes.FailedPrecondition, "version mismatch")
		}
		return nil, status.Errorf(codes.Internal, "compare_and_set: %v", err)
	}
	return s.response(out, 0), nil
}

// resolveExpiry converts a client-supplied relative TTL into the absolute
// timestamp a replicated command carries, resolved once here (by whichever
// node's API edge received the request, before proposing) rather than
// independently by each replica at apply time — see engine.ApplyPut.
func resolveExpiry(ttlSeconds int64) int64 {
	if ttlSeconds <= 0 {
		return 0
	}
	return time.Now().Add(time.Duration(ttlSeconds) * time.Second).UnixNano()
}

func (s *StateServer) replicatedResponse(result replication.ApplyResult, proposeErr error, op string) (*nativev1.MutationResponse, error) {
	if proposeErr != nil {
		// This covers raft.ErrNotLeader/ErrLeadershipLost among others —
		// TR-004's fencing in practice: raft itself refuses to commit
		// through a node that isn't (or is no longer) the leader.
		return nil, status.Errorf(codes.Unavailable, "%s: %v", op, proposeErr)
	}
	if result.Err != nil {
		if errors.Is(result.Err, engine.ErrVersionMismatch) {
			return nil, status.Error(codes.FailedPrecondition, "version mismatch")
		}
		return nil, status.Errorf(codes.Internal, "%s: %v", op, result.Err)
	}
	return s.response(result.Outcome, result.Term), nil
}

func (s *StateServer) response(out engine.Outcome, epoch uint64) *nativev1.MutationResponse {
	spec := s.Registry.Spec()
	if s.Metrics != nil {
		s.Metrics.SetCommitPosition(spec.ID, out.CommitPosition)
	}
	return &nativev1.MutationResponse{
		NamespaceId:       spec.ID,
		PartitionId:       0,
		Epoch:             epoch,
		CommitPosition:    out.CommitPosition,
		DurabilityProfile: spec.Profile,
		Deduplicated:      out.Deduplicated,
		Version:           out.Version,
	}
}

// checkNamespace rejects requests addressed to a namespace this node
// doesn't serve, explicitly (TR-010), instead of silently operating
// against whatever namespace_id the client happened to send.
func checkNamespace(reg *catalog.Registry, namespaceID string) error {
	if err := reg.Validate(namespaceID); err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	return nil
}
