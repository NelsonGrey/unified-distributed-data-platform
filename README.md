# Unified Distributed Data Platform

Proposed product and architecture package for an independently buildable distributed data platform. The working thesis is deliberately narrower than “replace Redis, Kafka, Hazelcast, and a graph database at once”: prove a dependable key-value and durable-log substrate first, with one control plane and explicit workload policies; add query and graph capabilities only after feasibility gates pass.

## Documents

- [Business Requirements](docs/BUSINESS_REQUIREMENTS.md)
- [Technical Requirements](docs/TECHNICAL_REQUIREMENTS.md)
- [Domain-Driven Design](docs/DOMAIN_DRIVEN_DESIGN.md)
- [Traceability and Validation](docs/TRACEABILITY_AND_VALIDATION.md)
- [Sources and Assumptions](docs/SOURCES_AND_ASSUMPTIONS.md)

## Status

Implementation targets the fastest defensible go-to-market slice, not the full MVP scope in one pass: a **single-node, `cache`-profile** deployment that proves BR-003 (atomic state + change record, no customer-managed dual write) for the cache-invalidation/fan-out beachhead sub-segment — deliberately without the most expensive, slowest part of the roadmap (partition replication/consensus), since `cache`-profile semantics don't require it. See the critical-path rationale in the project history for why replication is staged after this, not before it.

No benchmark result, compatibility certification, security assessment, compliance certification, SLA, or production-readiness claim is implied. Replication, the `durable`/`strong` profiles, multi-node control plane, and Redis/Kafka compatibility adapters are not yet built.

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
- `internal/catalog` — validates a namespace's workload profile and rejects requests to a namespace this node doesn't serve (TR-010).
- `api/native/v1` — versioned gRPC API definitions (TRD 6): `StateService` (KV) and `StreamService` (change stream/consumer offsets).
- `internal/api` — gRPC services adapting the engine to the native API.
- `cmd/uddp-node` — single-node runtime binary.
- `cmd/uddpctl` — local development CLI.
- `deploy/kubernetes` — StatefulSet + headless Service for a single-node deployment.
