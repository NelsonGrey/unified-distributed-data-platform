package api

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/marknelson/uddp/api/admin/v1"
	"github.com/marknelson/uddp/internal/backup"
	"github.com/marknelson/uddp/internal/catalog"
	"github.com/marknelson/uddp/internal/engine"
	"github.com/marknelson/uddp/internal/replication"
)

// AdminServer implements adminv1.AdminServiceServer. Registry and Engine
// are always required; Node is optional (nil for a non-replicated,
// single-node deployment) — see each method for how behavior adapts.
type AdminServer struct {
	adminv1.UnimplementedAdminServiceServer

	Engine   *engine.Engine
	Registry *catalog.Registry
	Node     *replication.Node // nil if this node isn't replicated
}

func (s *AdminServer) AddNode(_ context.Context, req *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
	if s.Node == nil {
		return nil, errNotReplicated
	}
	if req.Id == "" || req.RaftAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "id and raft_addr are required")
	}
	if err := s.Node.AddVoter(req.Id, req.RaftAddr); err != nil {
		return nil, adminError("add_node", err)
	}
	cluster, err := s.currentCluster()
	if err != nil {
		return nil, err
	}
	return &adminv1.AddNodeResponse{Cluster: cluster}, nil
}

func (s *AdminServer) RemoveNode(_ context.Context, req *adminv1.RemoveNodeRequest) (*adminv1.RemoveNodeResponse, error) {
	if s.Node == nil {
		return nil, errNotReplicated
	}
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if err := s.Node.RemoveServer(req.Id); err != nil {
		return nil, adminError("remove_node", err)
	}
	cluster, err := s.currentCluster()
	if err != nil {
		return nil, err
	}
	return &adminv1.RemoveNodeResponse{Cluster: cluster}, nil
}

func (s *AdminServer) ListNodes(_ context.Context, _ *adminv1.ListNodesRequest) (*adminv1.ListNodesResponse, error) {
	if s.Node == nil {
		return nil, errNotReplicated
	}
	cluster, err := s.currentCluster()
	if err != nil {
		return nil, err
	}
	return &adminv1.ListNodesResponse{Cluster: cluster}, nil
}

func (s *AdminServer) currentCluster() (*adminv1.Cluster, error) {
	servers, err := s.Node.ListServers()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list_nodes: %v", err)
	}
	nodes := make([]*adminv1.Node, len(servers.Members))
	for i, m := range servers.Members {
		nodes[i] = &adminv1.Node{Id: m.ID, RaftAddr: m.Addr, IsVoter: m.IsVoter}
	}
	return &adminv1.Cluster{Nodes: nodes, LeaderId: servers.LeaderID, LeaderRaftAddr: servers.LeaderAddr}, nil
}

func (s *AdminServer) CreateBackup(_ context.Context, req *adminv1.CreateBackupRequest) (*adminv1.CreateBackupResponse, error) {
	if req.Path == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}
	spec := s.Registry.Spec()
	entries := s.Engine.Snapshot()
	meta, err := backup.WriteFile(req.Path, backup.Meta{
		NamespaceID:   spec.ID,
		RecoveryPoint: s.Engine.LastCommitPosition(),
	}, entries)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create_backup: %v", err)
	}
	return &adminv1.CreateBackupResponse{Backup: backupInfo(meta)}, nil
}

func (s *AdminServer) RestoreBackup(_ context.Context, req *adminv1.RestoreBackupRequest) (*adminv1.RestoreBackupResponse, error) {
	if req.Path == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}
	meta, entries, err := backup.ReadFile(req.Path)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "restore_backup: %v", err)
	}

	if s.Node != nil {
		// Replicate the load so every node's state stays consistent —
		// see OpLoadSnapshot's doc comment for why a direct engine call
		// here would desync followers.
		result, err := s.Node.Propose(replication.Command{Op: replication.OpLoadSnapshot, Entries: entries})
		if err != nil {
			return nil, adminError("restore_backup", err)
		}
		if result.Err != nil {
			return nil, status.Errorf(codes.Internal, "restore_backup: %v", result.Err)
		}
	} else {
		if err := s.Engine.Reset(); err != nil {
			return nil, status.Errorf(codes.Internal, "restore_backup: reset: %v", err)
		}
		for _, e := range entries {
			if _, err := s.Engine.ApplyPut(e.Key, e.Value, e.ExpiresAtUnixNano, ""); err != nil {
				return nil, status.Errorf(codes.Internal, "restore_backup: load key %q: %v", e.Key, err)
			}
		}
	}

	return &adminv1.RestoreBackupResponse{Backup: backupInfo(meta)}, nil
}

func backupInfo(meta backup.Meta) *adminv1.BackupInfo {
	return &adminv1.BackupInfo{
		NamespaceId:   meta.NamespaceID,
		RecoveryPoint: meta.RecoveryPoint,
		EntryCount:    int32(meta.EntryCount),
		CreatedAt:     meta.CreatedAt.Format(time.RFC3339),
		Sha256Entries: meta.SHA256Entries,
	}
}

func (s *AdminServer) ExportNamespace(_ context.Context, _ *adminv1.ExportNamespaceRequest) (*adminv1.ExportNamespaceResponse, error) {
	entries := s.Engine.Snapshot()
	out := make([]*adminv1.ExportedEntry, len(entries))
	for i, e := range entries {
		out[i] = &adminv1.ExportedEntry{Key: e.Key, Value: e.Value, ExpiresAtUnixNano: e.ExpiresAtUnixNano}
	}
	return &adminv1.ExportNamespaceResponse{Entries: out}, nil
}

func (s *AdminServer) DeleteNamespace(_ context.Context, req *adminv1.DeleteNamespaceRequest) (*adminv1.DeleteNamespaceResponse, error) {
	spec := s.Registry.Spec()
	if req.ConfirmNamespaceId != spec.ID {
		return nil, status.Errorf(codes.InvalidArgument, "confirm_namespace_id must equal %q", spec.ID)
	}

	before := len(s.Engine.Snapshot())

	if s.Node != nil {
		result, err := s.Node.Propose(replication.Command{Op: replication.OpWipeAll})
		if err != nil {
			return nil, adminError("delete_namespace", err)
		}
		if result.Err != nil {
			return nil, status.Errorf(codes.Internal, "delete_namespace: %v", result.Err)
		}
	} else {
		if err := s.Engine.Reset(); err != nil {
			return nil, status.Errorf(codes.Internal, "delete_namespace: %v", err)
		}
	}

	return &adminv1.DeleteNamespaceResponse{EntriesDeleted: int32(before)}, nil
}

func (s *AdminServer) GetUsage(_ context.Context, _ *adminv1.GetUsageRequest) (*adminv1.GetUsageResponse, error) {
	spec := s.Registry.Spec()
	entries := s.Engine.Snapshot()
	var approxBytes int64
	for _, e := range entries {
		approxBytes += int64(len(e.Key) + len(e.Value))
	}
	return &adminv1.GetUsageResponse{
		NamespaceId: spec.ID,
		KeyCount:    int64(len(entries)),
		ApproxBytes: approxBytes,
	}, nil
}

var errNotReplicated = status.Error(codes.FailedPrecondition, "this node is not replicated; there is no cluster to administer")

// adminError maps a raft-level failure (most commonly "not leader") to the
// same UNAVAILABLE-with-reason pattern StateServer uses for writes, so
// clients handle both the data and admin planes the same way.
func adminError(op string, err error) error {
	return status.Errorf(codes.Unavailable, "%s: %v", op, err)
}
