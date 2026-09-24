// Package api adapts the internal engine to the native gRPC StateService
// (api/native/v1). It is the "API edge" component from TRD 4.1, scoped to
// the single-node, single-partition slice: namespace/quota/auth enforcement
// beyond a fixed partition/epoch are later-slice concerns.
package api

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nativev1 "github.com/marknelson/uddp/api/native/v1"
	"github.com/marknelson/uddp/internal/engine"
	"github.com/marknelson/uddp/internal/observability"
)

// StateServer implements nativev1.StateServiceServer over a single local
// engine instance representing partition 0, epoch 0.
type StateServer struct {
	nativev1.UnimplementedStateServiceServer

	Engine            *engine.Engine
	NamespaceID       string
	DurabilityProfile string
	Metrics           *observability.Metrics // optional; nil disables commit-position gauge updates
}

func (s *StateServer) Get(_ context.Context, req *nativev1.GetRequest) (*nativev1.GetResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	value, version, found := s.Engine.Get(req.Key)
	return &nativev1.GetResponse{Value: value, Version: version, Found: found}, nil
}

func (s *StateServer) Put(_ context.Context, req *nativev1.PutRequest) (*nativev1.MutationResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	out, err := s.Engine.Put(req.Key, req.Value, req.TtlSeconds, req.IdempotencyKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	return s.response(out), nil
}

func (s *StateServer) Delete(_ context.Context, req *nativev1.DeleteRequest) (*nativev1.MutationResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	out, err := s.Engine.Delete(req.Key, req.IdempotencyKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return s.response(out), nil
}

func (s *StateServer) CompareAndSet(_ context.Context, req *nativev1.CompareAndSetRequest) (*nativev1.MutationResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	out, err := s.Engine.CompareAndSet(req.Key, req.Value, req.ExpectedVersion, req.TtlSeconds, req.IdempotencyKey)
	if err != nil {
		if errors.Is(err, engine.ErrVersionMismatch) {
			return nil, status.Error(codes.FailedPrecondition, "version mismatch")
		}
		return nil, status.Errorf(codes.Internal, "compare_and_set: %v", err)
	}
	return s.response(out), nil
}

func (s *StateServer) response(out engine.Outcome) *nativev1.MutationResponse {
	if s.Metrics != nil {
		s.Metrics.SetCommitPosition(s.NamespaceID, out.CommitPosition)
	}
	return &nativev1.MutationResponse{
		NamespaceId:       s.NamespaceID,
		PartitionId:       0,
		Epoch:             0,
		CommitPosition:    out.CommitPosition,
		DurabilityProfile: s.DurabilityProfile,
		Deduplicated:      out.Deduplicated,
		Version:           out.Version,
	}
}
