// Command uddp-node runs the single-node local development runtime
// described by BR-001/TR-020: one process serving the native StateService
// API over a WAL-backed engine, with the same logical API the clustered
// deployment will use.
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
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	nativev1 "github.com/marknelson/uddp/api/native/v1"
	internalapi "github.com/marknelson/uddp/internal/api"
	"github.com/marknelson/uddp/internal/auth"
	"github.com/marknelson/uddp/internal/catalog"
	"github.com/marknelson/uddp/internal/engine"
	"github.com/marknelson/uddp/internal/observability"
	"github.com/marknelson/uddp/internal/streaming"
)

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:7070", "gRPC listen address")
		httpAddr    = flag.String("http-addr", "127.0.0.1:7071", "HTTP listen address for /healthz and /metrics")
		dataDir     = flag.String("data-dir", "./data", "directory holding the WAL and local state")
		namespaceID = flag.String("namespace", "default", "namespace served by this single-partition node")
		// cache is the only profile this single-node build can honestly
		// report: durable/strong require replica quorum (TRD 4.3), which
		// doesn't exist until delivery slice 2.
		profile = flag.String("profile", "cache", "durability profile reported in responses (cache only, until replication ships)")

		tlsCert = flag.String("tls-cert", "", "path to a PEM-encoded TLS certificate; if empty, the gRPC listener is plaintext (fine for local dev bound to 127.0.0.1, not for anything else)")
		tlsKey  = flag.String("tls-key", "", "path to the PEM-encoded private key for --tls-cert")

		authToken = flag.String("auth-token", "", "shared bearer token required on every RPC (prefer the UDDP_AUTH_TOKEN env var over this flag, which is visible in the process list); if empty, no authentication is enforced")
	)
	flag.Parse()

	if *authToken == "" {
		*authToken = os.Getenv("UDDP_AUTH_TOKEN")
	}

	if err := run(*addr, *httpAddr, *dataDir, *namespaceID, *profile, *tlsCert, *tlsKey, *authToken); err != nil {
		log.Fatalf("uddp-node: %v", err)
	}
}

func run(addr, httpAddr, dataDir, namespaceID, profile, tlsCert, tlsKey, authToken string) error {
	spec, err := catalog.NewNamespaceSpec(namespaceID, profile)
	if err != nil {
		return err
	}
	registry := catalog.NewRegistry(spec)

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	walPath := filepath.Join(dataDir, "partition-0.wal")
	offsetsPath := filepath.Join(dataDir, "consumer-offsets.wal")

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

	metrics := observability.New()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	var serverOpts []grpc.ServerOption

	tlsEnabled := tlsCert != "" || tlsKey != ""
	if tlsEnabled {
		if tlsCert == "" || tlsKey == "" {
			return fmt.Errorf("both --tls-cert and --tls-key must be set together")
		}
		cert, err := tls.LoadX509KeyPair(tlsCert, tlsKey)
		if err != nil {
			return fmt.Errorf("load TLS keypair: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})))
	} else {
		log.Println("uddp-node: WARNING starting without TLS (--tls-cert/--tls-key not set) — traffic is plaintext; only safe for local development")
	}

	interceptors := []grpc.UnaryServerInterceptor{metrics.UnaryServerInterceptor()}
	if authToken != "" {
		interceptors = append(interceptors, auth.UnaryServerInterceptor(authToken))
	} else {
		log.Println("uddp-node: WARNING starting without authentication (--auth-token/UDDP_AUTH_TOKEN not set) — any client can read and write every key")
	}
	serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(interceptors...))

	grpcServer := grpc.NewServer(serverOpts...)
	nativev1.RegisterStateServiceServer(grpcServer, &internalapi.StateServer{
		Engine:   eng,
		Registry: registry,
		Metrics:  metrics,
	})
	nativev1.RegisterStreamServiceServer(grpcServer, &internalapi.StreamServer{
		Engine:   eng,
		Offsets:  offsets,
		Registry: registry,
	})

	// The engine and offset store recovered successfully above, so this
	// process is ready to serve. TR-015 distinguishes ready/degraded/
	// recovering/unknown; a single-node process that reaches this line has
	// no basis to report anything but ready or (on shutdown) not-serving —
	// degraded/recovering/under-replicated states require replica state
	// that doesn't exist until delivery slice 2.
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	httpMux := http.NewServeMux()
	httpMux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	httpMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ready"}`))
	})
	httpServer := &http.Server{Addr: httpAddr, Handler: httpMux}

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

	log.Printf("uddp-node: serving namespace %q (profile=%s) on %s (grpc, tls=%v) / %s (http), wal=%s", namespaceID, profile, addr, tlsEnabled, httpAddr, walPath)
	return grpcServer.Serve(lis)
}
