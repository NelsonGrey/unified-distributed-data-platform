package api

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nativev1 "github.com/marknelson/uddp/api/native/v1"
	"github.com/marknelson/uddp/internal/catalog"
	"github.com/marknelson/uddp/internal/engine"
	"github.com/marknelson/uddp/internal/streaming"
	"github.com/marknelson/uddp/internal/wal"
)

// StreamServer implements nativev1.StreamServiceServer over a single local
// engine's change stream and its consumer-group offset store.
type StreamServer struct {
	nativev1.UnimplementedStreamServiceServer

	Engine   *engine.Engine
	Offsets  *streaming.OffsetStore
	Registry *catalog.Registry
}

const defaultMaxRecords = 500

func (s *StreamServer) Fetch(_ context.Context, req *nativev1.FetchRequest) (*nativev1.FetchResponse, error) {
	if err := checkNamespace(s.Registry, req.NamespaceId); err != nil {
		return nil, err
	}
	if req.FromOffset == 0 {
		return nil, status.Error(codes.InvalidArgument, "from_offset must be >= 1")
	}
	limit := int(req.MaxRecords)
	if limit <= 0 {
		limit = defaultMaxRecords
	}

	recs, err := s.Engine.Fetch(req.FromOffset, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "fetch: %v", err)
	}

	out := make([]*nativev1.ChangeRecord, len(recs))
	nextOffset := req.FromOffset
	for i, r := range recs {
		out[i] = &nativev1.ChangeRecord{
			Offset: r.Offset,
			Kind:   toChangeKind(r.Kind),
			Key:    r.Key,
			Value:  r.Value,
		}
		nextOffset = r.Offset + 1
	}

	return &nativev1.FetchResponse{Records: out, NextOffset: nextOffset}, nil
}

func toChangeKind(k wal.RecordKind) nativev1.ChangeKind {
	switch k {
	case wal.KindPut:
		return nativev1.ChangeKind_CHANGE_KIND_PUT
	case wal.KindDelete:
		return nativev1.ChangeKind_CHANGE_KIND_DELETE
	default:
		return nativev1.ChangeKind_CHANGE_KIND_UNSPECIFIED
	}
}

func (s *StreamServer) CommitOffset(_ context.Context, req *nativev1.CommitOffsetRequest) (*nativev1.CommitOffsetResponse, error) {
	if err := checkNamespace(s.Registry, req.NamespaceId); err != nil {
		return nil, err
	}
	if req.ConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "consumer_group must not be empty")
	}
	committed, err := s.Offsets.Commit(req.ConsumerGroup, req.Offset)
	if err != nil {
		if errors.Is(err, streaming.ErrOffsetRegression) {
			return nil, status.Errorf(codes.FailedPrecondition, "offset %d is behind the committed offset %d for group %q", req.Offset, committed, req.ConsumerGroup)
		}
		return nil, status.Errorf(codes.Internal, "commit_offset: %v", err)
	}
	return &nativev1.CommitOffsetResponse{CommittedOffset: committed}, nil
}

func (s *StreamServer) FetchOffset(_ context.Context, req *nativev1.FetchOffsetRequest) (*nativev1.FetchOffsetResponse, error) {
	if err := checkNamespace(s.Registry, req.NamespaceId); err != nil {
		return nil, err
	}
	if req.ConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "consumer_group must not be empty")
	}
	offset, found := s.Offsets.Fetch(req.ConsumerGroup)
	return &nativev1.FetchOffsetResponse{Offset: offset, Found: found}, nil
}
