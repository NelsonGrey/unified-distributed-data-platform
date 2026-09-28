# Upgrade Matrix

**Status:** Started, not qualified. This documents the current policy and mechanism, not a proven upgrade path — see BR-011/TR-011 in the [TRD](TECHNICAL_REQUIREMENTS.md).

## Current policy

There is exactly one supported on-disk WAL format version (`1`). There is no released version history yet, so there is nothing to qualify an upgrade *between* — this document exists to state the policy and mechanism now, before that history exists, rather than retrofit it later.

## Mechanism

Every WAL file (the engine's own log, and the raft log store) carries a 4-byte magic plus a 1-byte format version, written when the file is created (`internal/wal`). `Open` validates both:

- Wrong magic → rejected (not a uddp WAL file).
- Version mismatch → rejected, explicitly, with the version found and the version supported. This build does not attempt to read a format it doesn't recognize, and does not silently ignore the mismatch (TR-011: "unsupported paths shall be blocked").

This is the *mechanism* TR-008/TR-011 need — automatic rejection of an unsupported on-disk format — not the full requirement. Still missing:

- A published matrix of qualified upgrade paths between specific released versions (none exist yet).
- Rolling/zero-downtime upgrade orchestration (TR-008's "expose predicted movement, risk, compatibility, and abort boundaries before execution") — today, upgrading a node means stopping the process, replacing the binary, and restarting it; there is no control-plane-driven rolling upgrade workflow.
- A downgrade path — none is supported or tested in either direction.

## What to do until this is qualified

Don't run mixed format versions in one cluster, and don't assume a future version of this software will read today's WAL files without an explicit, tested migration path being published first.
