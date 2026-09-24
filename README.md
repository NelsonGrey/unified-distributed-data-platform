# Unified Distributed Data Platform

Proposed product and architecture package for an independently buildable distributed data platform. The working thesis is deliberately narrower than “replace Redis, Kafka, Hazelcast, and a graph database at once”: prove a dependable key-value and durable-log substrate first, with one control plane and explicit workload policies; add query and graph capabilities only after feasibility gates pass.

## Documents

- [Business Requirements](docs/BUSINESS_REQUIREMENTS.md)
- [Technical Requirements](docs/TECHNICAL_REQUIREMENTS.md)
- [Domain-Driven Design](docs/DOMAIN_DRIVEN_DESIGN.md)
- [Traceability and Validation](docs/TRACEABILITY_AND_VALIDATION.md)
- [Sources and Assumptions](docs/SOURCES_AND_ASSUMPTIONS.md)

## Status

Started from the fastest defensible go-to-market slice — a single-node, `cache`-profile deployment proving BR-003 (atomic state + change record, no customer-managed dual write) — and has since added raft-based partition replication (delivery slice 2), which is what makes the `durable` profile (WAL durability plus replica quorum, TRD 4.3) an honest claim rather than an aspirational one.

No benchmark result, compatibility certification, security assessment, compliance certification, SLA, or production-readiness claim is implied. Multi-partition placement, a control plane, log compaction, and Redis/Kafka compatibility adapters are not yet built. See `internal/replication`'s package doc for what's explicitly deferred within replication itself.

## Building and running

```sh
go build ./...
go test ./...

go run ./cmd/uddp-node --data-dir=./data --addr=127.0.0.1:7070 --http-addr=127.0.0.1:7071

# KV surface
go run ./cmd/uddpctl --addr=127.0.0.1:7070 put mykey myvalue
go run ./cmd/uddpctl --addr=127.0.0.1:7070 get mykey

# Change stream — the same commit, no separate write
go run ./cmd/uddpctl --addr=127.0.0.1:7070 fetch 1
go run ./cmd/uddpctl --addr=127.0.0.1:7070 commit-offset my-consumer 1
go run ./cmd/uddpctl --addr=127.0.0.1:7070 offset my-consumer

# Health and metrics
curl http://127.0.0.1:7071/healthz
curl http://127.0.0.1:7071/metrics
```

### TLS and authentication

By default the gRPC listener is plaintext with no authentication — fine for local development bound to `127.0.0.1`, not for anything else. Enable both for any deployment beyond that:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 1 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

UDDP_AUTH_TOKEN=s3cret go run ./cmd/uddp-node --tls-cert=cert.pem --tls-key=key.pem

UDDP_TOKEN=s3cret go run ./cmd/uddpctl --tls --tls-ca=cert.pem get mykey
```

Prefer the `UDDP_AUTH_TOKEN`/`UDDP_TOKEN` env vars over `--auth-token`/`--token` — flag values are visible in the process list. This is a shared bearer token, not the mTLS/OIDC workload identity TRD §7 specifies as the real mechanism — it exists so nothing is left unauthenticated while that's staged for a later slice.

### Replicated cluster (`durable` profile)

Three nodes, one bootstrapping the cluster:

```sh
PEERS="n1=127.0.0.1:18101,n2=127.0.0.1:18102,n3=127.0.0.1:18103"

go run ./cmd/uddp-node --data-dir=./data/n1 --addr=127.0.0.1:17101 --http-addr=127.0.0.1:17111 \
  --profile=durable --raft-id=n1 --raft-addr=127.0.0.1:18101 --raft-peers="$PEERS" --raft-bootstrap

go run ./cmd/uddp-node --data-dir=./data/n2 --addr=127.0.0.1:17102 --http-addr=127.0.0.1:17112 \
  --profile=durable --raft-id=n2 --raft-addr=127.0.0.1:18102 --raft-peers="$PEERS"

go run ./cmd/uddp-node --data-dir=./data/n3 --addr=127.0.0.1:17103 --http-addr=127.0.0.1:17113 \
  --profile=durable --raft-id=n3 --raft-addr=127.0.0.1:18103 --raft-peers="$PEERS"

go run ./cmd/uddpctl --addr=127.0.0.1:17101 put session:1 online   # committed at position 1 (profile=durable)
go run ./cmd/uddpctl --addr=127.0.0.1:17102 get session:1          # replicated: online (version=1)
go run ./cmd/uddpctl --addr=127.0.0.1:17102 put session:2 online   # rejected: node is not the leader
```

`--raft-bootstrap` forms the cluster and is set on exactly one node, only when first creating it. `durable` is only accepted when `--raft-peers` is set (`cache` works with or without replication). See `internal/replication`'s package doc for what this covers and what's still deferred (log compaction, multi-partition, read-index linearizable reads).

### Performance qualification harness (TR-019)

```sh
go run ./cmd/uddp-bench --addr=127.0.0.1:7070 \
  --read-ratio=0.8 --key-cardinality=100000 --concurrency=32 \
  --warmup=10s --duration=60s \
  --target-note="single-node cache, <hardware>, <uddp-node version/commit>" \
  --output-json=report.json
```

Supports read/write/mixed workloads (`--read-ratio`), hot-key concentration (`--hot-key-count`), and TTL churn (`--ttl-seconds`), reporting p50/p95/p99/p99.9 latency, throughput, and errors per operation. `--target-note` is where you record what was actually being benchmarked (topology, durability profile, hardware) — a report without that context isn't reproducible evidence, whatever the numbers say.

**A report from this tool is not by itself an approved performance claim.** TRD §9 requires a fixed, approved baseline environment and multiple repetitions before any number is publishable (BR-010) — this tool produces the raw measurement, not the qualification process around it. See `internal/benchmark`'s package doc for what's covered (the workload/percentile/environment capture) versus explicitly deferred (fault injection while loaded, automatic saturation detection).

### Kubernetes

```sh
docker build -t uddp-node:local .
kubectl apply -k deploy/kubernetes
```

See [deploy/kubernetes/README.md](deploy/kubernetes/README.md) for details and current limitations (single replica only; TLS/auth exist but aren't wired into the manifests yet).

## Code layout

Follows the [DDD](docs/DOMAIN_DRIVEN_DESIGN.md#8-code-organization-guidance) organization guidance:

- `internal/wal` — checksummed append-only log with crash/torn-tail recovery (TR-005) and offset-indexed range reads for the change stream.
- `internal/engine` — deterministic single-partition state machine (TR-002, TR-003) built on the WAL; state mutation and change record share one commit (TRD 4.5).
- `internal/streaming` — durable, restart-safe consumer-group offset tracking.
- `internal/observability` — Prometheus metrics and gRPC health checking (TR-014/TR-015), scoped to what a single, unreplicated node can honestly report.
- `internal/auth` — shared bearer token authentication for every RPC (health checks exempted), a smaller stand-in for the mTLS/OIDC workload identity in TRD §7.
- `internal/catalog` — validates a namespace's workload profile and rejects requests to a namespace this node doesn't serve (TR-010); gates the `durable` profile on replication actually being enabled.
- `internal/replication` — raft-backed partition replication (delivery slice 2): FSM adapting the engine to `raft.FSM`, cluster bootstrap/join, quorum-committed proposals.
- `internal/benchmark` — TR-019 performance qualification harness: workload generation, percentile/error/throughput stats, environment capture.
- `api/native/v1` — versioned gRPC API definitions (TRD 6): `StateService` (KV) and `StreamService` (change stream/consumer offsets).
- `internal/api` — gRPC services adapting the engine to the native API.
- `cmd/uddp-node` — single-node runtime binary.
- `cmd/uddpctl` — local development CLI.
- `cmd/uddp-bench` — performance qualification harness CLI (TR-019).
- `deploy/kubernetes` — StatefulSet + headless Service for a single-node deployment.
