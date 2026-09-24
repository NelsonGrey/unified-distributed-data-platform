package api

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/marknelson/uddp/api/admin/v1"
	"github.com/marknelson/uddp/internal/replication"
)

// AdminServer implements adminv1.AdminServiceServer over a replicated
// node's cluster membership. It has no meaning without replication — a
// single-node, non-replicated uddp-node doesn't register this service at
// all (see cmd/uddp-node), rather than expose an Admin API that can only
// ever return "there is one node."
type AdminServer struct {
	adminv1.UnimplementedAdminServiceServer

	Node *replication.Node
}

func (s *AdminServer) AddNode(_ context.Context, req *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
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

// adminError maps a raft-level failure (most commonly "not leader") to the
// same UNAVAILABLE-with-reason pattern StateServer uses for writes, so
// clients handle both the data and admin planes the same way.
func adminError(op string, err error) error {
	return status.Errorf(codes.Unavailable, "%s: %v", op, err)
}
