# gRPC Bidirectional Streaming Lab

An experimental Go project exploring delivery-semantics trade-offs in bidirectional gRPC streaming, with instrumentation for measuring duplicates, acks, backoff, and reconnect behavior under injected failures.

## Problem Space

In production distributed systems, the question "did the command arrive, and how many times?" is not academic. This lab implements a harness that:

- Sends commands over bidirectional gRPC streams
- Tracks delivered/duplicated/acknowledged counts
- Measures reconnect behavior and backoff timing
- Compares strategies (raw vs. idempotent vs. effective-once) under failure conditions

## Architecture
┌─────────────┐ bidirectional stream ┌─────────────┐ │ Client │◄────── grpc.ServerStream ───►│ Server │ │ (harness) │ │ (handler) │ └─────────────┘ └─────────────┘ │ │ ▼ ▼ Metrics (sent/acked/dupes) Request handling Backoff timing State persistence


### Components

| Path | Purpose |
|------|---------|
| `proto/rig/v1/` | Protocol Buffer definitions (message schemas, service interface) |
| `internal/server/` | Server-side gRPC handler implementations |
| `cmd/client/` | Client harness: sends messages, measures metrics, injects failures |
| `pkg/metrics/` | Counters for duplicates, acks, reconnects, backoff intervals |
| `configs/` | Delivery-strategy configurations (raw, idempotent, effective-once) |

## What Gets Measured

| Metric | Meaning |
|--------|---------|
| `messages_sent` | Total messages transmitted from client |
| `messages_delivered` | Messages received by server (may exceed sent due to retransmission) |
| `messages_acked` | Messages successfully acknowledged by server |
| `duplicates_detected` | Messages received after deduplication logic (if enabled) |
| `reconnect_count` | Number of stream re-establishments |
| `backoff_duration_ms` | Cumulative backoff time across reconnects |
| `deadline_exceeded_count` | Timeout failures injected or observed |

## Running the Experiment

```bash
# Start the server
go run ./cmd/server

# Run client with raw strategy (baseline)
go run ./cmd/client --strategy=raw --duration=30s

# Run with idempotent strategy (server deduplicates)
go run ./cmd/client --strategy=idempotent --duration=30s

# Inject failures (disconnect at 15s, measure recovery)
go run ./cmd/client --strategy=raw --inject-disconnect=true --duration=60s 
```


### Typical Output

=== STRATEGY: raw ===
Messages Sent:      1200
Messages Delivered: 1287 (7.3% duplicate transmission)
Messages Acked:     1200
Reconnects:         4
Total Backoff:      245ms
Deadline Exceeded:  3

=== STRATEGY: idempotent ===
Messages Sent:      1200
Messages Delivered: 1287 (wire-level duplicates preserved)
Messages Deduped:   87 (unique: 1200)
Messages Acked:     1200
Reconnects:         4
Total Backoff:      245ms

Delivery Strategy Comparison
Strategy	Duplicate Handling	Best For
raw	Server delivers wire-level duplicates unchanged	Baseline measurement
idempotent	Client includes ID, server deduplicates before processing	Idempotent workflows
effective_once	Client tracks acks, retransmits unacknowledged only	Critical command delivery
Design Decisions
DCAS Pattern for Idempotency
The server uses compare-and-swap (DCAS) semantics when deduplicating:

// Pseudocode for idempotent deduplication
func ProcessMessage(ctx Context, msg Message) (Result, error) {
    return db.Upsert(msg.ID, payload, condition="NOT EXISTS")
}

This ensures stale data issues are prevented without locks or distributed transactions.

Backoff Strategy
Exponential backoff with jitter on reconnect. Tested ranges: 100ms–2s per reconnect attempt.

Why This Matters for Robotics/AI Infrastructure
Robotics fleets face similar challenges:

Commands to robots need delivery guarantees (did the gripper move?)
Telemetry streams tolerate some loss but must detect duplicates
Disconnected networks (factory floors) require resilient reconnection
This lab's metrics directly map to fleet-management concerns.

Future Work
Persistent logging of message traces for forensic replay
Integration with OpenTelemetry for distributed tracing
Side-by-side comparison with Kafka/RabbitMQ at-least-once delivery
Multi-client load test (N clients, 1 server)
License
MIT — open for experimentation and extension.

