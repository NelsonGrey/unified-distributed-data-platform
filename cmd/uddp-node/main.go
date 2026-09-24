// Command uddp-node runs the single-node local development runtime
// described by BR-001/TR-020: one process serving the native StateService
// API over a WAL-backed engine, with the same logical API the clustered
// deployment will use.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc"

	nativev1 "github.com/marknelson/uddp/api/native/v1"
	internalapi "github.com/marknelson/uddp/internal/api"
	"github.com/marknelson/uddp/internal/engine"
	"github.com/marknelson/uddp/internal/streaming"
)

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:7070", "gRPC listen address")
		dataDir     = flag.String("data-dir", "./data", "directory holding the WAL and local state")
		namespaceID = flag.String("namespace", "default", "namespace served by this single-partition node")
		// cache is the only profile this single-node build can honestly
		// report: durable/strong require replica quorum (TRD 4.3), which
		// doesn't exist until delivery slice 2.
		profile = flag.String("profile", "cache", "durability profile reported in responses (cache only, until replication ships)")
	)
	flag.Parse()

	if err := run(*addr, *dataDir, *namespaceID, *profile); err != nil {
		log.Fatalf("uddp-node: %v", err)
	}
}

func run(addr, dataDir, namespaceID, profile string) error {
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

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	nativev1.RegisterStateServiceServer(grpcServer, &internalapi.StateServer{
		Engine:            eng,
		NamespaceID:       namespaceID,
		DurabilityProfile: profile,
	})
	nativev1.RegisterStreamServiceServer(grpcServer, &internalapi.StreamServer{
		Engine:  eng,
		Offsets: offsets,
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("uddp-node: shutting down")
		grpcServer.GracefulStop()
	}()

	log.Printf("uddp-node: serving namespace %q (profile=%s) on %s, wal=%s", namespaceID, profile, addr, walPath)
	return grpcServer.Serve(lis)
}
