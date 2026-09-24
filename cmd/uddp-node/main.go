// Command uddp-node runs the single-node local development runtime
// described by BR-001/TR-020: one process serving the native StateService
// API over a WAL-backed engine, with the same logical API the clustered
// deployment will use. It can also run as one member of a raft-replicated
// cluster (delivery slice 2) via --raft-*.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	adminv1 "github.com/marknelson/uddp/api/admin/v1"
	nativev1 "github.com/marknelson/uddp/api/native/v1"
	internalapi "github.com/marknelson/uddp/internal/api"
	"github.com/marknelson/uddp/internal/auth"
	"github.com/marknelson/uddp/internal/catalog"
	"github.com/marknelson/uddp/internal/engine"
	"github.com/marknelson/uddp/internal/observability"
	"github.com/marknelson/uddp/internal/replication"
	"github.com/marknelson/uddp/internal/streaming"
)

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:7070", "gRPC listen address")
		httpAddr    = flag.String("http-addr", "127.0.0.1:7071", "HTTP listen address for /healthz and /metrics")
		dataDir     = flag.String("data-dir", "./data", "directory holding the WAL and local state")
		namespaceID = flag.String("namespace", "default", "namespace served by this single-partition node")
		profile     = flag.String("profile", "cache", "durability profile reported in responses (cache always; durable requires --raft-peers)")

		tlsCert = flag.String("tls-cert", "", "path to a PEM-encoded TLS certificate; if empty, the gRPC listener is plaintext (fine for local dev bound to 127.0.0.1, not for anything else)")
		tlsKey  = flag.String("tls-key", "", "path to the PEM-encoded private key for --tls-cert")

		authToken = flag.String("auth-token", "", "shared bearer token required on every RPC (prefer the UDDP_AUTH_TOKEN env var over this flag, which is visible in the process list); if empty, no authentication is enforced")

		raftID        = flag.String("raft-id", "", "this node's raft server ID (required if --raft-peers is set)")
		raftAddr      = flag.String("raft-addr", "", "address this node's raft transport listens on (required if --raft-peers is set)")
		raftPeers     = flag.String("raft-peers", "", "comma-separated id=addr pairs for every node in the cluster, including this one; enables replication when set")
		raftBootstrap = flag.Bool("raft-bootstrap", false, "form a new cluster from --raft-peers (set on exactly one node, only when first creating the cluster)")

		raftTLSCert = flag.String("raft-tls-cert", "", "this node's certificate for mutual TLS between raft nodes; if empty, inter-node raft traffic is plaintext (fine for local dev, not for a real network)")
		raftTLSKey  = flag.String("raft-tls-key", "", "private key for --raft-tls-cert")
		raftTLSCA   = flag.String("raft-tls-ca", "", "CA certificate that signed every node's --raft-tls-cert, used to verify peers in both directions")
	)
	flag.Parse()

	if *authToken == "" {
		*authToken = os.Getenv("UDDP_AUTH_TOKEN")
	}

	peers, err := parsePeers(*raftPeers)
	if err != nil {
		log.Fatalf("uddp-node: %v", err)
	}

	if err := run(nodeConfig{
		addr: *addr, httpAddr: *httpAddr, dataDir: *dataDir, namespaceID: *namespaceID, profile: *profile,
		tlsCert: *tlsCert, tlsKey: *tlsKey, authToken: *authToken,
		raftID: *raftID, raftAddr: *raftAddr, raftPeers: peers, raftBootstrap: *raftBootstrap,
		raftTLSCert: *raftTLSCert, raftTLSKey: *raftTLSKey, raftTLSCA: *raftTLSCA,
	}); err != nil {
		log.Fatalf("uddp-node: %v", err)
	}
}

func parsePeers(s string) ([]replication.Peer, error) {
	if s == "" {
		return nil, nil
	}
	var peers []replication.Peer
	for _, entry := range strings.Split(s, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid --raft-peers entry %q, want id=addr", entry)
		}
		peers = append(peers, replication.Peer{ID: parts[0], Addr: parts[1]})
	}
	return peers, nil
}

type nodeConfig struct {
	addr, httpAddr, dataDir, namespaceID, profile string
	tlsCert, tlsKey, authToken                    string
	raftID, raftAddr                              string
	raftPeers                                     []replication.Peer
	raftBootstrap                                 bool
	raftTLSCert, raftTLSKey, raftTLSCA            string
}

func run(cfg nodeConfig) error {
	replicationEnabled := len(cfg.raftPeers) > 0
	if replicationEnabled && (cfg.raftID == "" || cfg.raftAddr == "") {
		return fmt.Errorf("--raft-id and --raft-addr are required when --raft-peers is set")
	}

	spec, err := catalog.NewNamespaceSpec(cfg.namespaceID, cfg.profile, replicationEnabled)
	if err != nil {
		return err
	}
	registry := catalog.NewRegistry(spec)

	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		return err
	}
	walPath := filepath.Join(cfg.dataDir, "partition-0.wal")
	offsetsPath := filepath.Join(cfg.dataDir, "consumer-offsets.wal")

	eng, err := engine.Open(walPath, nil)
	if err != nil {
		return err
	}
	defer eng.Close()

	offsets, err := streaming.Open(offsetsPath)
	if err != nil {
		return err
	}
	defer offsets.Close()

	var replNode *replication.Node
	if replicationEnabled {
		var raftTLS *replication.TLSConfig
		raftTLSFlagsSet := cfg.raftTLSCert != "" || cfg.raftTLSKey != "" || cfg.raftTLSCA != ""
		if raftTLSFlagsSet {
			if cfg.raftTLSCert == "" || cfg.raftTLSKey == "" || cfg.raftTLSCA == "" {
				return fmt.Errorf("--raft-tls-cert, --raft-tls-key, and --raft-tls-ca must be set together")
			}
			raftTLS = &replication.TLSConfig{CertFile: cfg.raftTLSCert, KeyFile: cfg.raftTLSKey, PeerCAFile: cfg.raftTLSCA}
		}

		replNode, err = replication.Open(eng, replication.Config{
			ID:        cfg.raftID,
			BindAddr:  cfg.raftAddr,
			DataDir:   filepath.Join(cfg.dataDir, "raft"),
			Bootstrap: cfg.raftBootstrap,
			Peers:     cfg.raftPeers,
			LogOutput: os.Stderr,
			TLS:       raftTLS,
		})
		if err != nil {
			return fmt.Errorf("start replication: %w", err)
		}
		defer replNode.Shutdown()
	}

	metrics := observability.New()

	lis, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return err
	}

	var serverOpts []grpc.ServerOption

	tlsEnabled := cfg.tlsCert != "" || cfg.tlsKey != ""
	if tlsEnabled {
		if cfg.tlsCert == "" || cfg.tlsKey == "" {
			return fmt.Errorf("both --tls-cert and --tls-key must be set together")
		}
		cert, err := tls.LoadX509KeyPair(cfg.tlsCert, cfg.tlsKey)
		if err != nil {
			return fmt.Errorf("load TLS keypair: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})))
	} else {
		log.Println("uddp-node: WARNING starting without TLS (--tls-cert/--tls-key not set) — traffic is plaintext; only safe for local development")
	}

	interceptors := []grpc.UnaryServerInterceptor{metrics.UnaryServerInterceptor()}
	if cfg.authToken != "" {
		interceptors = append(interceptors, auth.UnaryServerInterceptor(cfg.authToken))
	} else {
		log.Println("uddp-node: WARNING starting without authentication (--auth-token/UDDP_AUTH_TOKEN not set) — any client can read and write every key")
	}
	serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(interceptors...))

	grpcServer := grpc.NewServer(serverOpts...)
	nativev1.RegisterStateServiceServer(grpcServer, &internalapi.StateServer{
		Engine:      eng,
		Registry:    registry,
		Metrics:     metrics,
		Replication: replNode,
	})
	nativev1.RegisterStreamServiceServer(grpcServer, &internalapi.StreamServer{
		Engine:   eng,
		Offsets:  offsets,
		Registry: registry,
	})
	if replNode != nil {
		adminv1.RegisterAdminServiceServer(grpcServer, &internalapi.AdminServer{Node: replNode})
	}

	// The engine and offset store recovered successfully above, so this
	// process is ready to serve. TR-015 distinguishes ready/degraded/
	// recovering/unknown; without replication there's no basis to report
	// anything but ready or (on shutdown) not-serving. With replication
	// enabled this still just means "this process is up," not "this node
	// is the leader" — readers should check StateServer responses/metrics
	// for that, not health.
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	httpMux := http.NewServeMux()
	httpMux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	httpMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ready"}`))
	})
	httpServer := &http.Server{Addr: cfg.httpAddr, Handler: httpMux}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("uddp-node: http server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("uddp-node: shutting down")
		healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		httpServer.Shutdown(context.Background())
		grpcServer.GracefulStop()
	}()

	log.Printf("uddp-node: serving namespace %q (profile=%s, replicated=%v) on %s (grpc, tls=%v) / %s (http), wal=%s",
		cfg.namespaceID, cfg.profile, replicationEnabled, cfg.addr, tlsEnabled, cfg.httpAddr, walPath)
	return grpcServer.Serve(lis)
}
