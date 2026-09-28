# Redis Compatibility

**Status:** Started, not qualified. This is the declared subset and its current test coverage — see BR-006/TR-009 in the [BRD](BUSINESS_REQUIREMENTS.md)/[TRD](TECHNICAL_REQUIREMENTS.md) for the full requirement, and TRD §4.1: "Redis RESP and Kafka protocol adapters implement declared subsets only."

## Declared subset

| Command | Support | Notes |
|---|---|---|
| `PING` | Supported | |
| `GET key` | Supported | |
| `SET key value` | Supported | |
| `SET key value EX seconds` | Supported | Maps to the engine's TTL |
| `SET key value <any other option>` | Explicit error | `NX`, `XX`, `PX`, `KEEPTTL`, etc. are not implemented — rejected, not silently ignored |
| `DEL key [key ...]` | Supported | |
| Everything else | Explicit `ERR unknown command` | Includes `EXPIRE`, `TTL`, `INCR`, `MULTI`/`EXEC`, `SUBSCRIBE`, `AUTH`, `SELECT`, `HELLO`, and all hash/list/set/sorted-set commands |

Every command outside this table returns a RESP error naming the command, never an approximation or a silent no-op (TR-010).

## What this does not mean

- **Not a Redis replacement.** This is an on-ramp for the small set of operations listed above, not a compatibility layer for a real Redis workload.
- **No RESP3.** Only the RESP2 wire format is implemented (what `SET`/`GET`/`PING`/`DEL` actually need).
- **No TLS/auth on this listener.** Unlike the native gRPC API, the RESP listener (`--redis-addr`) has no transport security or authentication wired in yet — treat it as trusted-network-only.
- **No namespace routing.** The RESP listener operates on whatever single namespace this node serves; there's no `SELECT`-style multi-database concept.
- **No transactions, pub/sub, scripting, or persistence commands** (`SAVE`, `BGSAVE`, etc.) — none of these exist.

## Evidence

`internal/resp`'s test suite covers the parser against real RESP2 wire bytes and each supported command's dispatch behavior, including the explicit-error path for unsupported commands. It was also live-verified against the actual `redis-cli` client (not just this repo's own test suite) for `PING`, `SET`, `GET`, `SET ... EX`, `DEL`, and an unsupported command. There is no executable compatibility corpus yet in the TR-009 sense (a versioned, automated differential test suite run against a real Redis instance) — that's the next step to actually qualify this, not just start it.
