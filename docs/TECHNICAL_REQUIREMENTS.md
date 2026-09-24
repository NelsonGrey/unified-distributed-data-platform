# Technical Requirements Document

**Version:** 0.1 · **Status:** Proposed / architecture discovery · **Date:** 2026-09-24

Related: [BRD](BUSINESS_REQUIREMENTS.md) · [DDD](DOMAIN_DRIVEN_DESIGN.md) · [Validation](TRACEABILITY_AND_VALIDATION.md)

## Implementation status (as of 2026-09-24)

Engineering-level status against this document's own components and requirements. See the BRD for the business-facing view and the DDD for domain/bounded-context status.

**Architecture (§1):** the modular monolith/data-node shape is what's built (`cmd/uddp-node`); the "logically separate control plane" doesn't exist yet — cluster topology is static CLI configuration, not a reconciled desired-state system.

**Workload profiles (§3):** `cache` and `durable` are both implemented and namespace-validated (a node refuses to claim `durable` without replication actually running). `strong` remains gated, as specified.

**Components (§4):** 4.1 (API edge) — native gRPC implemented (`StateService`, `StreamService`, `AdminService`); no REST admin API, no protocol adapters. 4.2 (control plane) — first slice implemented: live cluster membership (`AdminService.AddNode`/`RemoveNode`/`ListNodes`), tested including a node catching up via full log replay after joining; no desired-state reconciliation, no consensus-backed metadata group separate from the data plane's own raft group, no `Operation` state machine. 4.3 (partition runtime) — single fixed partition, epoch/fencing via raft term (not a hand-rolled fencing token), no placement/zone-awareness (there's nothing to place yet — single partition). 4.4 (storage) — memory index + checksummed WAL implemented; no manifest/segment rotation, no compaction (explicitly deferred, see `internal/replication`'s package doc), object storage/backup not implemented. 4.5 (state+log integration) — implemented and tested (this is BR-003's mechanism). 4.6 — not started, as specified (not MVP).

**Security (§7):** TLS + token auth on the client API; mutual TLS between raft nodes (the one place real mTLS workload identity is actually implemented, vs. the bearer token elsewhere). No OIDC, no RBAC, no envelope encryption at rest, no audit log, no threat model review.

**Performance validation (§9):** the harness exists (`uddp-bench`) and captures the fields this section asks for; no qualification run against a fixed/approved environment has happened, so no number from it is a claim yet.

**Delivery slices (§10):** 1 (single-node engine) — done. 2 (replication/consensus/fencing) — done for the data plane (raft via hashicorp/raft, not hand-rolled), with a real but non-exhaustive fault test (leader kill, non-leader rejection); no formal fault harness. 3 (change log/consumers/atomicity/dedup) — done. 4 (control-plane workflows, backup/restore) — only the observability and Kubernetes-packaging pieces are done; control-plane workflows and backup/restore are not. 5 (security/isolation + Redis compat) — TLS/mTLS/auth done; Redis compatibility not started. 6 (Kafka compat) — not started.

## 1. Architecture objective

Deliver an independently testable state-and-event substrate whose guarantees are explicit and whose failure behavior is observable. The architecture starts as a modular monolith/data-node binary plus a logically separate control plane. Service decomposition is justified by scaling, isolation, or lifecycle evidence—not by a microservices preference.

```mermaid
flowchart LR
  C[Clients and migration tools] --> A[Native API and protocol adapters]
  A --> R[Router and admission control]
  R --> P[Partition runtime]
  P --> M[Memory index]
  P --> W[WAL and segment store]
  P --> L[Ordered change log]
  W --> B[Backup/export]
  CP[Control plane] --> R
  CP --> P
  P --> O[Metrics traces logs audit]
```

## 2. Design principles

- One authoritative partition commit path; derived indexes and exports cannot acknowledge ahead of it.
- Named workload profiles with finite, testable semantics.
- Separate control-plane availability from data-plane behavior.
- Deterministic state machines and injectable clocks/failures for tests.
- Stable IDs, versioned formats, idempotent commands, and resumable workflows.
- Safe defaults: encrypted transport, authentication required, deny-by-default authorization, bounded resources.

## 3. Workload profiles

| Profile   | Intended use                         | Acknowledgement                                                 | Recovery expectation                                     |
| --------- | ------------------------------------ | --------------------------------------------------------------- | -------------------------------------------------------- |
| `cache`   | Reconstructible low-latency state    | Primary memory acceptance; replication optional/configured      | Loss is possible and explicitly bounded by configuration |
| `durable` | Stateful application data and events | WAL durability plus replica quorum under the qualified topology | No acknowledged loss within the tested fault model       |
| `strong`  | Linearizable conditional state       | Consensus commit by partition group                             | Gated until formal semantics and fault tests pass        |

Profiles are namespace-level in MVP. Per-key policies, causal consistency, CRDTs, and active-active writes are deferred because their composition and operator burden are substantial.

## 4. Components

### 4.1 API edge

- Native versioned gRPC API; optional REST administration API.
- Redis RESP and Kafka protocol adapters implement declared subsets only.
- Authentication, authorization context, request size limits, quotas, deadlines, idempotency keys, and trace propagation.
- Stable error taxonomy: invalid, unauthorized, conflict, throttled, unavailable, deadline, unsupported, and indeterminate.

### 4.2 Metadata and control plane

- Authoritative desired state for clusters, nodes, tenants, namespaces, policies, partitions, releases, backups, and operations.
- Consensus-backed metadata group with fencing tokens for mutating controllers.
- Declarative reconciliation; every long-running operation has an ID, state machine, progress, cancellation rules, and audit history.
- The data plane continues according to its last valid configuration during temporary control-plane loss.

### 4.3 Partition runtime

- Fixed virtual partitions mapped to replication groups; placement is zone/rack aware.
- Epoch and leader/fencing checks prevent stale owners from accepting writes.
- Per-partition serialized commit ordering initially; parallelism occurs across partitions.
- Reconfiguration uses snapshot transfer plus incremental catch-up, then an atomic ownership transition.
- Hot-partition detection reports key/tenant contributors using privacy-bounded sampling.

### 4.4 Storage

- Memory-resident primary index for the qualified hot path.
- Checksummed, append-only WAL/segments on local durable media.
- Manifest records segment set, format version, high-water marks, and snapshot lineage.
- Background compaction is rate-limited and exposes amplification, debt, and latency impact.
- Crash recovery replays only validated records after the last valid checkpoint.
- Object storage is backup/export first; transparent cold reads are post-MVP.

### 4.5 State and log integration

Within one partition, a transaction record includes state mutations, optional change records, producer identity/sequence, and commit metadata. Commit makes the state version and log offset visible together. Consumers remain at-least-once by default; producer deduplication and transactional offset advancement can provide exactly-once effects only inside the defined boundary. Documentation must not generalize this to arbitrary external side effects.

### 4.6 Query and graph extensions

Not MVP. Extensions consume versioned partition snapshots/change streams and declare freshness. A future graph service may store adjacency lists colocated by vertex partition; cross-partition traversal requires budgets, continuation tokens, and partial-result semantics. SQL requires an explicit supported grammar and consistency model.

## 5. Technical requirements

| ID         | Requirement                                                                                                                                                         | Maps to        | Status |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ------ |
| UDP-TR-001 | Every acknowledged mutation shall be associated with namespace, partition, epoch, commit position, and profile outcome.                                             | BR-002, BR-003 | Implemented — `MutationResponse` carries all five; epoch is the raft term when replicated, 0 otherwise |
| UDP-TR-002 | State mutation and its enabled partition-local change record shall commit atomically.                                                                               | BR-003         | Implemented and tested |
| UDP-TR-003 | Retries with the same producer/idempotency identity shall not create additional committed mutations within the retention window.                                    | BR-003         | Partial — idempotency-key dedup implemented and tested; "retention window" doesn't apply (dedup state isn't pruned/bounded yet) |
| UDP-TR-004 | Partition ownership changes shall use epochs/fencing and shall reject stale writers.                                                                                | BR-002, BR-012 | Implemented — raft term is the epoch; non-leader/stale-leader writes are rejected, tested live (kill leader, confirm rejection + failover) |
| UDP-TR-005 | Recovery shall validate checksums, manifests, and monotonic commit positions before serving.                                                                        | BR-002, BR-008 | Partial — checksums and monotonic positions validated and tested (including torn-write and corruption cases); no manifest concept exists (no multi-segment storage yet) |
| UDP-TR-006 | Backup artifacts shall be encrypted, checksummed, catalogued, restorable to an isolated target, and tied to an evidenced recovery point.                            | BR-008         | Not started |
| UDP-TR-007 | Controllers shall be idempotent and resumable after process loss.                                                                                                   | BR-004         | Partial — the only "controller" so far is cluster membership (`AdminService`), and it's idempotent (raft treats re-adding/re-removing the same server as a no-op, tested) by construction, not because a resumable Operation state machine exists; there's nothing yet with enough steps to need one |
| UDP-TR-008 | Rebalance and upgrade plans shall expose predicted movement, risk, compatibility, and abort boundaries before execution.                                            | BR-004, BR-011 | Not started — membership changes (add/remove a node) are implemented but apply immediately with no plan/preview/abort step; rebalance and upgrade planning specifically remain untouched |
| UDP-TR-009 | Each protocol adapter shall be versioned against an executable compatibility corpus.                                                                                | BR-006         | Not started — no adapters |
| UDP-TR-010 | Unsupported commands, options, or semantics shall return explicit errors rather than approximate silently.                                                          | BR-006         | Partial — applied within what exists (unknown namespace, unsupported profile, wrong-leader writes all fail explicitly, tested); no protocol adapters to apply it to yet |
| UDP-TR-011 | Tenant and namespace authorization shall be enforced at API and internal service boundaries.                                                                        | BR-007         | Partial — namespace validation enforced and tested; no tenant concept, no per-namespace RBAC (single shared token today) |
| UDP-TR-012 | Secrets shall use an external secret provider or encrypted store and support rotation without plaintext logging.                                                    | BR-007         | Partial — the auth token and TLS keys are read from files/env vars (not logged), which is the minimum bar; no secret-provider integration or rotation support |
| UDP-TR-013 | Audit events shall be append-only, integrity-protected, queryable, and exportable under retention policy.                                                           | BR-007, BR-014 | Not started |
| UDP-TR-014 | Metrics shall include request rate/errors/duration, queueing, replication lag, under-replication, storage/compaction, rebalance, recovery, and resource saturation. | BR-004, BR-012 | Partial, deliberately scoped — request rate/errors/duration and commit position are implemented; replication lag/under-replication/compaction/rebalance/saturation are omitted rather than faked, since the signals they'd need don't exist yet (see `internal/observability`'s package doc) |
| UDP-TR-015 | Health shall distinguish ready, degraded, recovering, under-replicated, policy-violating, and unknown.                                                              | BR-012         | Partial, deliberately scoped — only ready/not-serving are meaningful today, for the same reason as TR-014; reporting a fake "degraded" would violate this same requirement's spirit |
| UDP-TR-016 | Resource quotas and admission control shall prevent one tenant/namespace from exhausting cluster-wide memory, disk, connections, or execution slots.                | BR-005, BR-007 | Not started |
| UDP-TR-017 | Usage meters shall be deterministic, replayable, versioned, and reconcilable to invoices/estimates.                                                                 | BR-005         | Not started |
| UDP-TR-018 | All persisted and transmitted schemas/formats shall carry versions with forward/backward compatibility rules and migration tests.                                   | BR-011, BR-014 | Not started — the gRPC proto is versioned (`v1`) by convention; no compatibility rules or migration tests exist yet |
| UDP-TR-019 | The benchmark harness shall capture environment, topology, data, workload, warmup, duration, percentiles, errors, recovery, and cost inputs.                        | BR-010         | Partial — environment, workload, warmup/duration, percentiles, and errors are captured; topology is manual (`--target-note`), recovery-while-loaded and cost inputs aren't captured (see `internal/benchmark`'s package doc) |
| UDP-TR-020 | The local runtime shall use the same logical APIs and formats while clearly reporting unsupported distributed behaviors.                                            | BR-001, BR-009 | Implemented — single-node and clustered modes share the same API/binary; `durable` is refused outright (not silently degraded) when replication isn't configured |
| UDP-TR-021 | Export and deletion jobs shall be asynchronous, idempotent, authorized, audited, and produce completion evidence.                                                   | BR-014         | Not started |
| UDP-TR-022 | Console flows shall support keyboard navigation, non-color status, 200% zoom, and programmatic labels.                                                              | BR-015         | Not applicable yet — no console exists |

## 6. API sketch

```proto
service StateService {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Put(PutRequest) returns (MutationResponse);
  rpc Delete(DeleteRequest) returns (MutationResponse);
  rpc CompareAndSet(CompareAndSetRequest) returns (MutationResponse);
  rpc Batch(BatchRequest) returns (BatchResponse); // one partition in MVP
}

message MutationResponse {
  string namespace_id = 1;
  uint32 partition_id = 2;
  uint64 epoch = 3;
  uint64 commit_position = 4;
  string durability_profile = 5;
  bool deduplicated = 6;
}
```

Stream APIs cover append, fetch, consumer-group join/heartbeat, offset commit, and retention administration. API specifications must define ordering, size limits, timeouts, partial failure, cancellation, and retry safety.

## 7. Security and privacy

- Workload identity via mTLS and/or short-lived OIDC tokens; human control-plane access via federated OIDC.
- RBAC scoped by organization, project, cluster, namespace, and operation; policy engine decisions are auditable.
- TLS in transit and envelope encryption at rest; key identifiers and rotation state are metadata, not secrets.
- Network policies separate public/API, management, replication, and storage traffic.
- Threat model covers tenant escape, confused deputy, stale leader, replay, protocol parser abuse, backup theft, supply chain, and control-plane compromise.
- Data classification, residency, retention, deletion, and telemetry minimization are configurable. “SOC 2/GDPR/HIPAA compliant” is prohibited without applicable assessment and organizational controls.

## 8. Reliability and operations

Required runbooks: node loss, zone loss, control-plane loss, disk exhaustion, corruption/checksum failure, replica divergence, hot partition, certificate expiry, failed upgrade, failed rebalance, backup failure, restore, and suspected credential compromise.

Every runbook specifies detection, impact by workload profile, safe containment, recovery, verification, rollback, and evidence retained.

## 9. Performance validation

No latency or throughput number is a requirement until a baseline environment is approved. Qualification includes:

- KV read-heavy, write-heavy, mixed, TTL churn, hot-key, and bounded-batch workloads.
- Log producer/consumer throughput, many partitions, consumer churn, retention, compaction, and replay.
- State-plus-change atomic path.
- Steady state and saturation; p50/p95/p99/p99.9; errors and timeouts.
- Node/zone failure, leader change, snapshot, compaction, rebalance, upgrade, backup, and restore while loaded.
- Comparison only against documented, equivalently durable/configured competitor modes.

## 10. Delivery slices

1. Deterministic single-node engine, WAL, recovery, native API, local tooling. — **Done**
2. Partition replication, metadata consensus, fencing, fault harness. — **Data-plane replication done** (raft via hashicorp/raft, real leader-failure/non-leader-rejection tests); no formal fault harness, no metadata-plane consensus (that's the control plane, still slice 4/not started)
3. Change log, consumers, state-change atomicity, deduplication. — **Done**
4. Control-plane workflows, observability, backup/restore, Kubernetes packaging. — **Observability and Kubernetes packaging done**; control-plane workflows have a first slice (live cluster membership — add/remove/list nodes, tested including a node catching up after joining); rebalance/upgrade planning and backup/restore not started
5. Security/isolation and declared Redis compatibility subset. — **TLS/mTLS/auth done**; Redis compatibility not started
6. Declared Kafka subset and migration tooling, if G1–G3 pass. — Not started (and its own precondition, G1–G3, hasn't formally passed either — see the BRD's gate status)
