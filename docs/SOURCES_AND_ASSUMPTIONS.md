# Sources and Assumptions

**Reviewed:** 2026-09-24

## 1. Source concept

The supplied Copilot transcript proposed one platform spanning key-value caching, durable streaming, graph queries, SQL/analytics, tiered storage, flexible consistency, managed/self-hosted deployment, protocol compatibility, and a unified control plane. It also proposed aggressive latency, exactly-once, availability, roadmap, and production-readiness language without implementation or benchmark evidence.

This package preserves the useful core—state plus event log, explicit policies, operational simplicity, compatibility on-ramps, observability, and predictable cost—but narrows the MVP and turns uncertain claims into feasibility gates.

## 2. Current primary references

- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) — RDB, AOF, combined, and no-persistence tradeoffs.
- [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/) — sharding, availability, client behavior, and acknowledged-write caveats.
- [Hazelcast architecture](https://docs.hazelcast.com/hazelcast/5.6/architecture/architecture) — partitioned in-memory data, AP/CP structures, stream processing, and SQL overlap.
- [Hazelcast persistence design](https://docs.hazelcast.com/hazelcast/5.6/storage/persistence-design) — log-structured persistence and compaction considerations.
- [Redpanda documentation](https://docs.redpanda.com/) — Kafka-compatible streaming, storage, transactions, and operational feature baseline to validate during implementation.
- [Apache Ignite documentation](https://ignite.apache.org/docs/latest/) — distributed cache/database, compute, SQL, and persistence baseline.
- [TigerGraph documentation](https://docs.tigergraph.com/) — graph schema, loading, query, and operational baseline.
- [Amazon ElastiCache documentation](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/WhatIs.html) — managed cache responsibilities and AWS-specific operating model.

These sources demonstrate that incumbents already overlap significantly. They do not validate UDDP differentiation or performance.

## 3. Assumptions requiring validation

- A meaningful customer segment wants one vendor for state and event logs.
- Partition-local atomic state-plus-change removes enough dual-write burden to justify migration.
- Named profiles are more usable than a combinatorial policy surface.
- A Redis subset can attract target workloads without creating unacceptable semantic risk.
- Customers value operational evidence and cost attribution enough to switch.
- The team can build and support a distributed storage engine safely.

## 4. Explicitly unverified

- Product name, trademarks, domain, licensing model, and open-source strategy.
- Target latency, throughput, scale, availability, RPO/RTO, and cost.
- Redis or Kafka compatibility level.
- Exactly-once effects outside the defined partition/consumer boundary.
- Cloud-provider support, certification, or compliance applicability.
- Market demand, willingness to pay, or competitive win rate.
