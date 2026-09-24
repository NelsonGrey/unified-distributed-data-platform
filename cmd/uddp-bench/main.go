// Command uddp-bench is the TR-019 performance qualification harness: it
// drives a configurable read/write/mixed/TTL-churn/hot-key workload
// against a running uddp-node and reports p50/p95/p99/p99.9 latency,
// throughput, and errors, with environment/workload metadata attached so a
// result is reproducible evidence, not just a number (BR-010).
//
// A result from this tool is not by itself an approved performance claim
// — TRD §9 requires a fixed, approved baseline environment and multiple
// repetitions before that. Treat --target-note as mandatory in practice:
// record what you were actually benchmarking (topology, durability
// profile, hardware) or the report is not reproducible evidence.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	nativev1 "github.com/marknelson/uddp/api/native/v1"
	"github.com/marknelson/uddp/internal/benchmark"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "uddp-bench:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("uddp-bench", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7070", "uddp-node gRPC address")
	namespace := fs.String("namespace", "default", "namespace id")
	useTLS := fs.Bool("tls", false, "use TLS to connect (implied by --tls-ca or --insecure-skip-verify)")
	tlsCA := fs.String("tls-ca", "", "path to a PEM CA certificate to trust, in addition to the system roots")
	insecureSkipVerify := fs.Bool("insecure-skip-verify", false, "skip server certificate verification (dev only)")
	token := fs.String("token", "", "bearer token (prefer UDDP_TOKEN env var — this flag is visible in the process list)")

	readRatio := fs.Float64("read-ratio", 0.5, "fraction of requests that are Get (0.0=all writes, 1.0=all reads)")
	keyCardinality := fs.Int("key-cardinality", 10000, "number of distinct keys in the working set")
	hotKeyCount := fs.Int("hot-key-count", 0, "if >0, the top N keys receive 80% of traffic (hot-key workload)")
	valueSize := fs.Int("value-size", 128, "size in bytes of generated values")
	ttlSeconds := fs.Int64("ttl-seconds", 0, "if >0, set on every write (TTL-churn workload when small relative to duration)")
	concurrency := fs.Int("concurrency", 16, "number of concurrent workers")
	warmup := fs.Duration("warmup", 5*time.Second, "warmup period excluded from measured results")
	duration := fs.Duration("duration", 30*time.Second, "measured run duration, after warmup")

	targetNote := fs.String("target-note", "", "free-text description of what's being benchmarked (topology, durability profile, hardware) — see the package doc; strongly recommended")
	outputJSON := fs.String("output-json", "", "if set, write the full report as JSON to this path")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *token == "" {
		*token = os.Getenv("UDDP_TOKEN")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	transportCreds, err := dialCredentials(*useTLS, *tlsCA, *insecureSkipVerify)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(transportCreds))
	if err != nil {
		return fmt.Errorf("dial %s: %w", *addr, err)
	}
	defer conn.Close()
	client := nativev1.NewStateServiceClient(conn)

	exec := &grpcExecutor{client: client, namespace: *namespace, token: *token}

	cfg := benchmark.RunConfig{
		Workload: benchmark.WorkloadConfig{
			ReadRatio:      *readRatio,
			KeyCardinality: *keyCardinality,
			HotKeyCount:    *hotKeyCount,
			ValueSize:      *valueSize,
			TTLSeconds:     *ttlSeconds,
		},
		Concurrency: *concurrency,
		Warmup:      *warmup,
		Duration:    *duration,
	}

	fmt.Fprintf(os.Stderr, "uddp-bench: warming up for %s, then measuring for %s...\n", *warmup, *duration)
	result := benchmark.Run(ctx, cfg, exec)

	env := benchmark.CaptureEnvironment(*addr, *targetNote)
	report := benchmark.NewReport(env, result)

	if err := report.WriteText(os.Stdout); err != nil {
		return err
	}

	if *outputJSON != "" {
		f, err := os.Create(*outputJSON)
		if err != nil {
			return fmt.Errorf("create %s: %w", *outputJSON, err)
		}
		defer f.Close()
		if err := report.WriteJSON(f); err != nil {
			return fmt.Errorf("write %s: %w", *outputJSON, err)
		}
		fmt.Fprintf(os.Stderr, "uddp-bench: wrote %s\n", *outputJSON)
	}

	return nil
}

// grpcExecutor implements benchmark.Executor against a real StateService.
type grpcExecutor struct {
	client    nativev1.StateServiceClient
	namespace string
	token     string
}

func (e *grpcExecutor) Execute(ctx context.Context, req benchmark.Request) error {
	if e.token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+e.token)
	}
	switch req.Op {
	case benchmark.OpGet:
		_, err := e.client.Get(ctx, &nativev1.GetRequest{NamespaceId: e.namespace, Key: req.Key})
		return err
	case benchmark.OpPut:
		_, err := e.client.Put(ctx, &nativev1.PutRequest{NamespaceId: e.namespace, Key: req.Key, Value: req.Value, TtlSeconds: req.TTLSeconds})
		return err
	default:
		return fmt.Errorf("uddp-bench: unknown op %v", req.Op)
	}
}

func dialCredentials(useTLS bool, caPath string, insecureSkipVerify bool) (credentials.TransportCredentials, error) {
	if !useTLS && caPath == "" && !insecureSkipVerify {
		return insecure.NewCredentials(), nil
	}

	cfg := &tls.Config{InsecureSkipVerify: insecureSkipVerify}
	if caPath != "" {
		pem, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read --tls-ca %s: %w", caPath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no valid certificates found in --tls-ca %s", caPath)
		}
		cfg.RootCAs = pool
	}
	return credentials.NewTLS(cfg), nil
}
