# Business Requirements Document

**Working name:** Unified Distributed Data Platform (UDDP)  
**Version:** 0.1  
**Status:** Proposed / discovery  
**Date:** 2026-09-24  
**Owner:** Mark Nelson

Related: [TRD](TECHNICAL_REQUIREMENTS.md) · [DDD](DOMAIN_DRIVEN_DESIGN.md) · [Validation](TRACEABILITY_AND_VALIDATION.md) · [Sources](SOURCES_AND_ASSUMPTIONS.md)

## Implementation status (as of 2026-09-24)

This is a code/test-level engineering status, not a gate approval — none of the go/no-go gates in §8 have been formally passed, and nothing here is a design-partner or usability-study result (BR-013's evidence requirement in particular is untouched). It exists so this document doesn't drift from what the repository actually does. See the TRD and DDD for the matching engineering/domain-level status.

**Built and tested:** single-node and 3-node raft-replicated deployment; `cache` and `durable` workload profiles (§5, `strong` still gated); namespaced KV (get/put/delete/CAS/TTL, not batch); atomic state-plus-change-record publication with no dual-write (BR-003 — the core thesis in §3.2); TLS + token auth on the client API and mutual TLS between cluster nodes; Prometheus metrics and health checks; a CLI and a performance-qualification harness with reproducible, environment-tagged output (BR-010's *mechanism*, not an approved claim); Kubernetes packaging (single-node, not live-cluster validated); CI running tests, race detection, and coverage on every change (BR-009).

**Not built:** most of the control plane (§4.2 in the TRD — provisioning/upgrade/rebalance workflows; live membership scaling is the one piece that exists), billing/cost-attribution (BR-005), Redis/Kafka compatibility adapters (BR-006), backup/restore (BR-008), an upgrade/downgrade matrix (BR-011), export/deletion tooling (BR-014), any web console (BR-015 is entirely unaddressed — there is no UI), and SDKs beyond generated gRPC stubs (BR proposes Java/Go first; only the raw generated Go client exists so far). Multi-partition placement and log compaction are also unimplemented, which bounds how far current evidence generalizes.

## 1. Executive summary

UDDP is a proposed developer-first platform for applications that need low-latency state and durable event streams without operating separate cache and log systems. Its first product slice combines a Redis-shaped key-value on-ramp, a partitioned durable log, workload-level durability profiles, local single-node development, and a unified control plane.

The product must earn expansion. SQL, continuous queries, graph traversal, object-store tiering, active-active multi-region operation, and broad protocol compatibility are staged options—not MVP promises. This corrects the source concept’s largest risk: attempting several mature database categories simultaneously before establishing one defensible workload.

## 2. Customer problem

Teams often assemble caches, event logs, stream processors, databases, and graph systems. The resulting stack duplicates data, security configuration, observability, operational expertise, and failure handling. Existing products already cover many individual and overlapping capabilities, so “multi-model” alone is not differentiation.

The initial problem is narrower: platform teams need a trustworthy shared state-and-event substrate with clear correctness choices, repeatable operations, and cost visibility. They need to know what an acknowledgement means, what can be lost, how failover behaves, and what a migration preserves.

## 3. Product thesis and differentiation

UDDP will compete on:

1. **Explicit workload contracts.** Each namespace selects a small, validated durability/consistency profile with documented acknowledgement, failure, RPO, and recovery semantics.
2. **One state-and-event identity.** A committed state mutation can emit an ordered change record without a customer-managed dual-write.
3. **Evidence-first operations.** The console explains replica health, lag, hot partitions, rebalance impact, recovery points, and cost drivers.
4. **Local-to-managed parity.** The same declarative namespace model works in a single-node developer runtime and clustered deployments, while topology-dependent behavior remains visibly different.
5. **Compatibility with declared limits.** Redis and Kafka adapters expose published, tested subsets. Unsupported semantics fail clearly; compatibility is never claimed from wire-level acceptance alone.

## 4. Target customers and users

### Beachhead

Small-to-medium platform teams building real-time applications that currently operate both a cache and an event log, especially gaming state, personalization, device/IoT state, operational counters, and fraud-feature pipelines.

### Personas

- Backend engineer: predictable APIs, local development, failure semantics, idiomatic SDKs.
- Platform engineer/SRE: safe provisioning, upgrades, rebalancing, backup, restore, and capacity evidence.
- Data engineer: ordered streams, replay, consumer progress, schemas, and export.
- Security administrator: tenant isolation, least privilege, auditability, key rotation.
- Engineering leader/FinOps owner: workload cost attribution and migration risk.

## 5. Scope

### MVP

- Single-node local runtime and a three-or-more-node clustered deployment.
- Namespaced key-value operations: get, put, delete, conditional update, TTL, bounded batch.
- Partitioned append-only log with producers, consumers, retention, replay, and consumer groups.
- Atomic state mutation plus change-record publication within one partition.
- Two initial workload profiles: `cache` and `durable`; a third `strong` profile is gated by consensus validation.
- Memory hot path plus local durable storage; explicit eviction and recovery behavior.
- Declarative control plane, CLI, administration API, metrics, traces, audit events, backup and restore.
- Java and Go SDKs first; protocol adapters are limited compatibility layers.
- Kubernetes deployment and a local container distribution.

### Post-MVP, gated

- Managed SaaS; additional clouds; active-active regions.
- SQL/continuous query execution.
- Graph adjacency and bounded traversal service.
- Object-store cold tier and analytical snapshots.
- Additional SDKs, connectors, and compatibility coverage.

### Non-goals for MVP

- Full Redis or Kafka behavioral equivalence.
- Arbitrary cross-partition ACID transactions.
- A general-purpose relational database, data warehouse, graph database, or ML platform.
- User-supplied arbitrary code in the data plane.
- Unqualified “exactly once,” “sub-millisecond,” “linear scaling,” or “zero downtime” claims.
- Compliance certification or production SLA before independent evidence exists.

## 6. Business requirements

| ID         | Requirement                                                                                                                                                | Priority | Acceptance evidence                                       | Status |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | -------: | --------------------------------------------------------- | ------ |
| UDP-BR-001 | A developer shall run the local runtime and complete a state-write/change-stream round trip in 15 minutes using public documentation.                      |     Must | Timed usability study with at least 10 target developers  | Partial — the round trip works and is fast in practice (README quickstart); no usability study run |
| UDP-BR-002 | The product shall publish precise acknowledgement, consistency, durability, failover, and data-loss semantics for every workload profile.                  |     Must | Approved semantic contract and fault-injection evidence   | Partial — `cache`/`durable` semantics documented (TRD §3) and covered by correctness tests (concurrent writes, leader failure, non-leader rejection); no formal fault-injection matrix or approval |
| UDP-BR-003 | A state mutation and its change record shall not require an application-managed dual-write when partition-local atomic publication is selected.            |     Must | End-to-end recovery and duplicate-delivery tests          | Implemented — atomic state+change commit, tested including restart recovery and idempotent-retry dedup |
| UDP-BR-004 | Operators shall provision, scale, upgrade, back up, restore, and inspect a cluster through one control plane.                                              |     Must | Scenario-based operator acceptance suite                  | Partial — "scale" and "inspect" are implemented (live add/remove/list nodes via `AdminService`, tested including catch-up after joining); provision, upgrade, back up, and restore are not; no scenario-based acceptance suite |
| UDP-BR-005 | Customers shall receive per-namespace usage, capacity, and cost-driver reporting.                                                                          |     Must | Billing reconciliation and allocation tests               | Not started |
| UDP-BR-006 | Redis/Kafka migration claims shall name the supported surface, semantic differences, test corpus, and fallback plan.                                       |     Must | Versioned compatibility matrices and migration tests      | Not started — no protocol adapters |
| UDP-BR-007 | Tenant data, credentials, management actions, and audit records shall be isolated and access-controlled.                                                   |     Must | Threat model, authorization tests, and independent review | Partial — TLS + shared-token auth (not per-tenant identity/RBAC), mTLS between cluster nodes; no audit log, no threat model review |
| UDP-BR-008 | Backup and restore shall be testable without production data and shall report recovery-point and recovery-time evidence.                                   |     Must | Scheduled restore exercises                               | Not started — no backup/export path; crash recovery from local WAL/raft log is tested, that's not the same thing |
| UDP-BR-009 | The MVP shall be independently buildable and testable with owned infrastructure and deterministic workloads; customer recruitment is not a prerequisite.   |     Must | Reproducible local/CI test environment                    | Implemented — CI runs build/vet/test(`-race`,`-cover`)/benchmark on every change |
| UDP-BR-010 | Public performance and cost claims shall identify hardware, topology, software versions, workload, data set, percentile, duration, and failure conditions. |     Must | Reproducible benchmark package and review                 | Partial — the harness (`uddp-bench`) captures all of that per-run; no claim has gone through a fixed-environment/repeated-run approval, and none should be read as validated yet |
| UDP-BR-011 | The product shall support rolling maintenance only for combinations proven by an upgrade matrix; unsupported paths shall be blocked.                       |     Must | Upgrade/downgrade qualification report                    | Not started |
| UDP-BR-012 | The product shall surface degraded, recovering, under-replicated, and policy-violating states without converting missing evidence into a healthy status.   |     Must | Fault-injection UI/API acceptance tests                   | Partial — health/metrics report only ready/not-serving (honestly, since replica-state signals for degraded/under-replicated don't exist yet); no UI, no fault-injection acceptance suite |
| UDP-BR-013 | The first commercial release shall serve one validated beachhead workload rather than market itself as a universal replacement.                            |     Must | Approved launch positioning and design-partner evidence   | Not started — no design partner engaged; see the beachhead framing in §4 for the intended target |
| UDP-BR-014 | Data export and deletion shall be documented, observable, and testable to reduce lock-in and privacy risk.                                                 |   Should | Portability and deletion verification tests               | Not started |
| UDP-BR-015 | Accessibility shall cover the web console’s keyboard use, focus order, contrast, zoom, and non-color status encoding.                                      |     Must | WCAG-oriented accessibility evaluation                    | Not started — no web console exists |

## 7. Core journeys

1. **Develop locally:** create a namespace, write state, consume its change stream, simulate restart, inspect recovery.
2. **Deploy safely:** declare topology and profile, receive a plan, provision, validate health, and obtain a readiness report.
3. **Migrate:** inventory commands/protocol features, run compatibility analysis, dual-read or shadow traffic, reconcile, cut over, and retain rollback.
4. **Respond to failure:** identify affected partitions and guarantees, contain, recover, verify, and export evidence.
5. **Control cost:** attribute RAM, disk, network, retention, and replica usage to namespaces and model changes before applying them.

## 8. Success measures and stage gates

Targets are hypotheses until measured.

- Developer activation: at least 80% of study participants complete the local journey unaided.
- Correctness: zero acknowledged-write loss outside the published profile contract in the qualified fault matrix.
- Recoverability: every release candidate completes automated backup restore plus a destructive cluster recovery exercise.
- Operability: median diagnosis time for the qualified top five incidents is under 15 minutes in operator studies.
- Compatibility: 100% pass rate for the declared subset; every known semantic deviation is documented.
- Economics: benchmark cost is reported by workload and SLO, not as one blended price claim.

### Go/no-go gates

- **G0 — thesis:** independently reproduce the state-plus-log value with deterministic workloads. *In progress — the mechanism is built and tested (unit + live multi-process smoke tests), but this hasn't been run as a deliberate, documented reproduction exercise against the gate's own bar.*
- **G1 — substrate:** storage, replication, recovery, and partition-local atomicity pass fault injection. *Partial — storage/replication/recovery/atomicity are implemented and covered by targeted correctness tests (crash recovery, torn-write tolerance, leader failure, concurrent-write races under `-race`); "fault injection" here means the qualified matrix in TRACEABILITY_AND_VALIDATION.md §3, which hasn't been run.*
- **G2 — operability:** upgrade, rebalance, backup/restore, isolation, and observability pass acceptance suites. *Not started — observability exists (metrics/health); upgrade, rebalance, backup/restore, and isolation do not.*
- **G3 — compatibility:** supported Redis/Kafka subsets and migration rollback are validated. *Not started.*
- **G4 — limited beta:** one beachhead workload shows better total operational outcome than its current two-system baseline. *Not started — no design partner.*
- **G5 — GA:** security review, capacity envelope, support model, and evidence-backed SLA are approved. *Not started.*

## 9. Commercial model

- Open local developer runtime and documented client/protocol specifications.
- Paid self-managed enterprise distribution may include advanced governance and support.
- Managed service is post-MVP and separately gated.
- Pricing should separate reserved memory, durable storage, retained log bytes, operations/throughput, network, and premium support; budget caps and estimators must use the same meters as billing.

No price, savings percentage, or SLA is approved in this document.

## 10. Risks

| Risk                                          | Response                                                                                 |
| --------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Scope collapse from building four databases   | Ship KV + log first; require evidence before query/graph expansion                       |
| Compatibility creates false confidence        | Publish command/feature/semantic matrices; test failures and rollback                    |
| Per-object policy causes unbounded complexity | Begin with named profiles, not arbitrary combinations                                    |
| Benchmarks reward synthetic cases             | Publish harness, tail latency, recovery behavior, saturation, and cost                   |
| Operational burden exceeds incumbents         | Treat day-two workflows as product requirements and release gates                        |
| Enterprise/security expectations arrive early | Threat-model the MVP; design identity, isolation, audit, and key rotation from inception |
