# bidi-rig

A gRPC bidirectional streaming laboratory in Go for studying delivery semantics
under failure: duplicates, ack accounting, reconnect behavior, and backoff
strategies — now with an optional AI-driven supervisor layer that makes
autonomous decisions during outages.

## Problem Space

In production distributed systems, the question "did the command arrive, and
how many times?" is not academic. bidi-rig is a harness that:

- Spawns N simulated clients over one bidirectional gRPC stream each
- Keeps message sequence numbers monotonic **across reconnects** (so
  duplicates and gaps after failover are detectable, not hidden by fresh state)
- Measures reconnect storms, backoff budgets, ack ratios, and duplicate rates
- Compares backoff strategies (raw vs. jittered) under injected failures
- Optionally delegates recovery decisions to a pluggable supervisor:
  a hand-tuned heuristic, or Jev (a System One decision model by TypeSafe AI)

## Architecture

```
┌──────────────────────┐                      ┌──────────────┐
│  cmd/client          │  stream(Ping/Pong)  │  cmd/server  │
│  N simulated clients │◄───────────────────►│  echo handler │
│  send loop / recv    │                      │  per-replica  │
│  totals counters     │                      │  identity     │
└─────────┬────────────┘                      └──────────────┘
          │ aggregates (atomics)
          ▼
┌─────────────────────────────────────────────────────────────┐
│  internal/supervisor                                         │
│                                                              │
│  snapshot (1s tick) ──► Supervisor ──► Decision ──► applier  │
│                          │             (JSONL log)            │
│        ┌─────────────────┴──────────────┐                    │
│        │                                │                    │
│   heuristic.Supervisor          jev.Client                 │
│   (thresholds + hysteresis)      (System One model,          │
│                                  circuit-breaker guarded)    │
└─────────────────────────────────────────────────────────────┘
```

The supervisor runs **off the hot path**: it samples aggregate counters once per
second and never sits inside the measured send/receive loops.

### Components

| Path | Purpose |
|------|---------|
| `proto/rig/v1/` | Protocol Buffers: `Rig.Session(stream Ping) returns (stream Pong)` |
| `internal/server/` | Server-side gRPC handler (echo with replica identity) |
| `cmd/client/` | Multi-client harness: senders, receivers, reconnect loop, failure injection, supervisor wiring |
| `internal/supervisor/` | Supervisor contract, circuit breaker, JSONL decision log |
| `internal/supervisor/heuristic/` | Threshold-based baseline supervisor (hysteresis, cooldowns) |
| `internal/supervisor/jev/` | Adapter for TypeSafe AI's Jev decision model |

### Protocol

```proto
message Ping {
  uint64 seq = 1;      // monotonic per client, survives reconnects
  bytes  payload = 2;   // tunable size for backpressure tests
}
message Pong {
  uint64 ack_seq = 1;            // echoes Ping.seq
  string server_id = 2;          // which replica served it
  int64  stream_open_unix_ms = 3;
  uint64 conn_stream_count = 4;
}
```

## What Gets Measured

| Metric | Meaning |
|--------|---------|
| `messages_sent` | Pings transmitted by all clients |
| `messages_acked` | Unique + duplicate pongs received (acks tracked by `ack_seq`) |
| `duplicates_detected` | `ack_seq` re-observations (duplicate delivery after reconnect) |
| `reconnect_count` | Stream re-establishments |
| `backoff_duration` | Cumulative time spent in backoff (raw vs. jittered comparison) |
| `deadline_exceeded` | Stream errors classified as context deadline exceeded |
| supervisor decisions | One JSONL record per second: action, source, confidence, latency, snapshot |

## Running

```bash
# 1. Start the server
go run ./cmd/server

# 2. Baseline: 10 clients, raw backoff, no supervision
go run ./cmd/client -clients 10 -rate-ms 100 -backoff raw -for 60s

# 3. Same scenario with the heuristic supervisor
go run ./cmd/client -clients 10 -rate-ms 100 -backoff raw -for 120s \\
    -supervisor heuristic -output output/heuristic/

# 4. Same scenario with the Jev supervisor
export TYPESAFE_API_KEY="..."
go run ./cmd/client -clients 10 -rate-ms 100 -backoff raw -for 120s \\
    -supervisor jev -output output/jev/

# 5. Unit tests
go test ./...
```

Induce outages by killing and restarting the server mid-run:

```bash
pkill -9 -f cmd/server && sleep 5 && go run ./cmd/server &
```

### Flags

| Flag | Meaning |
|------|---------|
| `-target` | gRPC server address |
| `-clients` | number of simulated clients |
| `-rate-ms` | ping interval per client |
| `-payload` | extra payload bytes per ping (backpressure tests) |
| `-backoff` | `raw` | `jittered` reconnect strategy |
| `-for` | run duration |
| `-supervisor` | `none` | `heuristic` | `jev` |
| `-output` | directory for JSONL decision logs |

## The Supervisor Layer

When `-supervisor` is enabled, a control loop samples aggregate run state once
per second and decides:

- `continue` — no action
- `switch_strategy` — flip `raw` ↔ `jittered` live (takes effect at next backoff)
- `force_reconnect` — broadcast: kill all client streams and reconnect immediately
- `abort_run` — terminate the run

Two implementations of the same interface are compared head-to-head:

- **Heuristic** — hand-written thresholds with hysteresis (consecutive bad
  windows), per-action cooldowns, and backoff-rate tracking. Fast, free,
  explainable, but hand-tuned.
- **Jev** — a System One model from TypeSafe AI that returns calibrated,
  typed decisions (choice/boolean/score) instead of generated text. It sees the
  JSON-serialized run snapshot and answers three questions: next action, is
  this an outage, and severity.

Every decision is appended to a JSONL log with its snapshot, source, latency,
confidence, and circuit-breaker state — making the heuristic-vs-Jev comparison
an empirical question rather than a claim.

### Failure containment: Jev is an optimization, not a dependency

The Jev client is wrapped in a circuit breaker (three-state: closed/open/
half-open) and a 5s timeout. On breaker-open or any error, decisions fall back
to the heuristic transparently — the run continues, decisions are simply
tagged `source: fallback` in the log. The rig is designed so that no external
service is a single point of failure.

### Known caveats

- `ack ratio` may exceed 1.0 under heavy duplication (pongs counted per receipt);
  treat >1.05 as a duplication signal, not a health signal
- `force_reconnect` is global (all clients flip together)
- the outcome field in decision logs is not yet populated (planned: correlate
  decisions with the next window's metrics)

## Roadmap

- Outcome correlation: fill `outcome` in decision logs from subsequent metric
  windows, enabling win-rate analysis by confidence bucket
- OpenTelemetry instrumentation of the supervisor loop
- Delivery-strategy variants beyond backoff (deduplication semantics,
  at-most-once vs at-least-once retransmission)
- Comparative analysis tooling over collected JSONL datasets

## License

MIT — open for experimentation and extension.
