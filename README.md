# Unified Distributed Data Platform

Proposed product and architecture package for an independently buildable distributed data platform. The working thesis is deliberately narrower than “replace Redis, Kafka, Hazelcast, and a graph database at once”: prove a dependable key-value and durable-log substrate first, with one control plane and explicit workload policies; add query and graph capabilities only after feasibility gates pass.

## Documents

- [Business Requirements](docs/BUSINESS_REQUIREMENTS.md)
- [Technical Requirements](docs/TECHNICAL_REQUIREMENTS.md)
- [Domain-Driven Design](docs/DOMAIN_DRIVEN_DESIGN.md)
- [Traceability and Validation](docs/TRACEABILITY_AND_VALIDATION.md)
- [Sources and Assumptions](docs/SOURCES_AND_ASSUMPTIONS.md)

## Status

Implementation has started against delivery slice 1 from the [TRD](docs/TECHNICAL_REQUIREMENTS.md#10-delivery-slices): a deterministic single-node engine (WAL, crash recovery, native gRPC API, local CLI). No benchmark result, compatibility certification, security assessment, compliance certification, SLA, or production-readiness claim is implied. Replication, control plane, streaming, and compatibility adapters are not yet built.

## Building and running (slice 1)

```sh
go build ./...
go test ./...

go run ./cmd/uddp-node --data-dir=./data --addr=127.0.0.1:7070
go run ./cmd/uddpctl --addr=127.0.0.1:7070 put mykey myvalue
go run ./cmd/uddpctl --addr=127.0.0.1:7070 get mykey
```

Code layout follows the [DDD](docs/DOMAIN_DRIVEN_DESIGN.md#8-code-organization-guidance) organization guidance:

- `internal/wal` — checksummed append-only log with crash/torn-tail recovery (TR-005).
- `internal/engine` — deterministic single-partition state machine (TR-002, TR-003) built on the WAL.
- `api/native/v1` — versioned gRPC API definitions (TRD 6).
- `internal/api` — gRPC service adapting the engine to the native API.
- `cmd/uddp-node` — single-node runtime binary.
- `cmd/uddpctl` — local development CLI.
