package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amangale/bidi-rig/internal/supervisor"
	"github.com/amangale/bidi-rig/internal/supervisor/heuristic"
	"github.com/amangale/bidi-rig/internal/supervisor/jev"
	rigv1 "github.com/amangale/bidi-rig/proto/rig/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	target   string
	n        int
	rate     time.Duration
	payload  int
	duration time.Duration
}

// Global tallies — client-side evidence for F2 (storm timing),
// F5 (duplicates/gaps after reconnect).
type totals struct {
	sent             atomic.Uint64
	acked            atomic.Uint64
	duplicates       atomic.Uint64
	reconnects       atomic.Uint64
	backoffSum       atomic.Int64  // microseconds spent in backoff (jittered vs raw)
	deadlineExceeded atomic.Uint64 // context.DeadlineExceeded-classified stream errors
}

// strategySwitcher holds the live backoff strategy ("raw" | "jittered").
// The supervisor flips this at runtime; backoff() must read it safely.
type strategySwitcher struct{ v atomic.Value } // string

func (s *strategySwitcher) get() string {
	v, _ := s.v.Load().(string)
	return v
}
func (s *strategySwitcher) set(str string) { s.v.Store(str) }

// reconnectBroadcaster: close-current, replace-new pattern broadcasts a
// "kill your streams now" signal to all clients. Clients see the channel
// close (not a value) so a single notify wakes every waiter.
type reconnectBroadcaster struct {
	mu sync.Mutex
	ch chan struct{}
}

func newReconnectBroadcaster() *reconnectBroadcaster {
	return &reconnectBroadcaster{ch: make(chan struct{})}
}
func (b *reconnectBroadcaster) notify() {
	b.mu.Lock()
	defer b.mu.Unlock()
	close(b.ch)
	b.ch = make(chan struct{})
}
func (b *reconnectBroadcaster) wait() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ch
}

var errForcedReconnect = errors.New("forced reconnect by supervisor")

func main() {
	const jevVersion = "jev-1.13.0"
	var (
		target   = flag.String("target", "localhost:50051", "server target")
		n        = flag.Int("clients", 10, "number of simulated clients")
		rateMs   = flag.Int("rate-ms", 100, "ping interval per client")
		payload  = flag.Int("payload", 0, "extra payload bytes per ping (F4)")
		strategy = flag.String("backoff", "raw", "raw | jittered (F2 toggle)")
		duration = flag.Duration("for", 60*time.Second, "run duration")
		supMode  = flag.String("supervisor", "none", "supervisor mode: none|heuristic|jev")
		output   = flag.String("output", "output/", "directory for decision logs")
	)
	flag.Parse()

	if *strategy != "raw" && *strategy != "jittered" {
		log.Fatalf("unknown backoff strategy %q (want raw|jittered)", *strategy)
	}
	if *supMode != "none" && *supMode != "heuristic" && *supMode != "jev" {
		log.Fatalf("unknown supervisor mode %q (want none|heuristic|jev)", *supMode)
	}

	cfg := &config{
		target:   *target,
		n:        *n,
		rate:     time.Duration(*rateMs) * time.Millisecond,
		payload:  *payload,
		duration: *duration,
	}

	sw := &strategySwitcher{}
	sw.set(*strategy)
	bcast := newReconnectBroadcaster()

	runID := fmt.Sprintf("run-%d", time.Now().Unix())
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	// --- Supervisor control plane ---
	var logStore *supervisor.LogStore
	var sup supervisor.Supervisor
	var t totals
	breaker := supervisor.NewBreaker(supervisor.DefaultBreakerConfig)
	fallback := heuristic.New(heuristic.DefaultConfig())

	if *supMode != "none" {
		var err error
		logStore, err = supervisor.NewLogStore(*output, "", 10)
		if err != nil {
			log.Fatalf("log store: %v", err)
		}
		defer logStore.Close()
		log.Printf("logging decisions to %s", logStore.Path())

		switch *supMode {
		case "heuristic":
			sup = fallback
		case "jev":
			apiKey := os.Getenv("TYPESAFE_API_KEY")
			if apiKey == "" {
				log.Fatal("TYPESAFE_API_KEY environment variable not set")
			}
			sup = jev.NewClient(apiKey, os.Getenv("JEV_BASE_URL"), "jev-1.13.0", breaker, 5*time.Second)
		}

		ctrlCh := make(chan supervisor.Decision, 10)

		// Applier: translates decisions into rig actions.
		go applyDecisions(runCtx, ctrlCh, cancelRun, sw, bcast)

		// Supervisor loop: samples aggregate totals each tick, decides, logs.
		go supervisorGoroutine(runCtx, runID, cfg, sw, &t /*placeholder*/, sup, fallback,
			logStore, breaker, ctrlCh)
	}

	deadline := time.Now().Add(cfg.duration)

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < cfg.n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runClient(id, cfg, &t, deadline, runCtx, sw, bcast)
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("client summary (%s strategy, run %s): sent=%d acked=%d duplicates=%d "+
		"reconnects=%d deadline_exceeded=%d backoff_time=%s over=%s\n",
		sw.get(), runID, t.sent.Load(), t.acked.Load(), t.duplicates.Load(),
		t.reconnects.Load(), t.deadlineExceeded.Load(),
		time.Duration(t.backoffSum.Load())*time.Microsecond, elapsed)
}

// applyDecisions maps supervisor decisions onto rig mechanics:
//
//	abort_run        → cancel run context (everything unwinds cleanly)
//	switch_strategy  → flip raw ↔ jittered; takes effect on NEXT backoff call
//	force_reconnect  → broadcast: kill all live streams, reconnect w/o backoff
func applyDecisions(ctx context.Context, ctrlCh <-chan supervisor.Decision,
	cancelRun context.CancelFunc, sw *strategySwitcher, bcast *reconnectBroadcaster) {

	for {
		select {
		case <-ctx.Done():
			return
		case d := <-ctrlCh:
			log.Printf("decision: %s (source=%s, confidence=%.2f)", d.Action, d.Source, d.Confidence)
			switch d.Action {
			case supervisor.ActionAbortRun:
				log.Printf("supervisor aborted run")
				cancelRun()
			case supervisor.ActionSwitchStrategy:
				next := "jittered"
				if sw.get() == "jittered" {
					next = "raw"
				}
				log.Printf("supervisor switching strategy %s -> %s", sw.get(), next)
				sw.set(next)
			case supervisor.ActionForceReconnect:
				log.Printf("supervisor forcing reconnect of all client streams")
				bcast.notify()
			case supervisor.ActionContinue:
				// no-op
			}
		}
	}
}

// supervisorGoroutine: one decision per second, driven by the GLOBAL aggregate
// snapshot. Never touches a client stream; decisions flow out via ctrlCh only.
func supervisorGoroutine(ctx context.Context, runID string, cfg *config, sw *strategySwitcher,
	t *totals, sup supervisor.Supervisor, fallback supervisor.Supervisor,
	logStore *supervisor.LogStore, breaker *supervisor.Breaker,
	ctrlCh chan<- supervisor.Decision) {

	start := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sent := t.sent.Load()
			acked := t.acked.Load()
			var ratio float64
			if sent > 0 {
				ratio = float64(acked) / float64(sent)
			}

			snap := supervisor.Snapshot{
				RunID:              runID,
				Strategy:           sw.get(),
				MessagesSent:       int64(sent),
				MessagesAcked:      int64(acked),
				AckedRatio:         ratio,
				DuplicatesDetected: int64(t.duplicates.Load()),
				ReconnectCount:     int64(t.reconnects.Load()),
				BackoffDurationMs:  t.backoffSum.Load() / 1000, // µs → ms
				DeadlineExceeded:   int64(t.deadlineExceeded.Load()),
				ElapsedMs:          time.Since(start).Milliseconds(),
			}

			d, err := sup.Decide(ctx, snap)
			if err != nil {
				if errors.Is(err, supervisor.ErrCircuitOpen) {
					log.Printf("circuit open — falling back to heuristic")
				} else {
					log.Printf("supervisor error (%v) — falling back to heuristic", err)
				}
				d, _ = fallback.Decide(ctx, snap)
				d.Source = "fallback"
			}

			entry := supervisor.DecisionEntry{
				Timestamp:    time.Now(),
				RunID:        runID,
				Source:       d.Source,
				Action:       d.Action,
				Confidence:   d.Confidence,
				LatencyMs:    d.LatencyMs,
				CircuitState: breaker.State(),
				Snapshot:     snap,
			}
			if err := logStore.Write(entry); err != nil {
				log.Printf("decision log write: %v", err)
			}

			select {
			case ctrlCh <- d:
			default:
				log.Printf("ctrlCh full — dropping decision %s", d.Action)
			}
		}
	}
}

// runClient drives one simulated client across any number of reconnects.
// seq is MONOTONIC ACROSS RECONNECTS — that's what makes F5 detectable:
// a fresh stream does not imply a fresh sequence.
func runClient(id int, cfg *config, t *totals, deadline time.Time,
	runCtx context.Context, sw *strategySwitcher, bcast *reconnectBroadcaster) {

	seq := uint64(0)
	seen := make(map[uint64]bool) // ack_seq -> observed; duplicates = re-observation
	var mu sync.Mutex             // guards seen during concurrent recvs (one per connection life)

	conn, err := grpc.NewClient(cfg.target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("client %d: dial: %v\n", id, err)
		return
	}
	defer conn.Close()

	first := true
	for time.Now().Before(deadline) && runCtx.Err() == nil {
		// Watch for supervisor-forced reconnects while the stream lives.
		forced := make(chan struct{}, 1)
		go func() {
			select {
			case <-bcast.wait():
				forced <- struct{}{}
			case <-runCtx.Done():
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Until(deadline)+2*time.Second)
		stream, err := rigv1.NewRigClient(conn).Session(ctx)
		if err != nil {
			cancel()
			wait := backoff(sw.get(), t.reconnects.Load(), &t.backoffSum)
			if first {
				log.Fatalf("client %d: initial dial: %v", id, err) // nothing to recover from
			}
			log.Printf("client %d: reconnect dial failed (%v) — retry in %s", id, err, wait)
			time.Sleep(wait)
			continue
		}
		if !first {
			t.reconnects.Add(1)
		}
		serr := sessionIO(stream, id, cfg, &seq, seen, &mu, t, deadline, forced)
		if serr != nil && errors.Is(serr, context.DeadlineExceeded) {
			t.deadlineExceeded.Add(1)
		}

		if errors.Is(serr, errForcedReconnect) {
			// Supervisor-ordered reconnect: immediate, no backoff, no storm.
			log.Printf("client %d: forced reconnect", id)
			cancel()
			first = false
			continue
		}

		if serr != nil {
			// Transport died (SIGKILL, LB cut, etc.) — F2 kicks in here.
			wait := backoff(sw.get(), t.reconnects.Load(), &t.backoffSum)
			log.Printf("client %d: stream error (%v) — reconnecting in %s\n", id, serr, wait)
			time.Sleep(wait)
		}
		if time.Now().After(deadline) || runCtx.Err() != nil {
			cancel() // release per-stream resources before sleeping/retrying
			break
		}
		log.Printf("client %d: — reconnected\n", id)
		first = false
		cancel() // release per-stream resources before sleeping/retrying
	}
}

// sessionIO runs one stream life: sender ticks pings, receiver tallies pongs.
// Returns the error that ended the stream (nil on clean close).
func sessionIO(stream rigv1.Rig_SessionClient, id int, cfg *config, seq *uint64,
	seen map[uint64]bool, mu *sync.Mutex, t *totals, deadline time.Time,
	forced <-chan struct{}) error {

	errCh := make(chan error, 3)
	var payload []byte
	if cfg.payload > 0 {
		payload = make([]byte, cfg.payload)
	}

	// Sender.
	go func() {
		tick := time.NewTicker(cfg.rate)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				if time.Now().After(deadline) {
					// half-close: tells the server no more pings.
					if err := stream.CloseSend(); err != nil {
						errCh <- err
						return
					}
					return
				}
				s := atomic.AddUint64(seq, 1)
				t.sent.Add(1)
				if err := stream.Send(&rigv1.Ping{Seq: s, Payload: payload}); err != nil {
					errCh <- err
					return
				}
			case <-stream.Context().Done():
				errCh <- stream.Context().Err()
				return
			case <-forced:
				// Supervisor fired the broadcast: end this stream's life now.
				errCh <- errForcedReconnect
				return
			}
		}
	}()

	// Receiver — sole tallyer of acks; dups/gaps computed at summary time.
	go func() {
		for {
			pong, err := stream.Recv()
			if err == io.EOF {
				errCh <- nil
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			t.acked.Add(1)
			mu.Lock()
			if seen[pong.AckSeq] {
				t.duplicates.Add(1)
			}
			seen[pong.AckSeq] = true
			mu.Unlock()
		}
	}()

	// Wait for either side to finish.
	return <-errCh
}

// backoff returns how long to wait before reconnecting.
// raw:      immediate retry (with a token pause so we don't spin) — this is
//
//	the behavior that makes reconnect storms catastrophic.
//
// jittered: full-jitter exponential backoff, capped at 5s — the control group.
func backoff(strategy string, attempts uint64, backoffSum *atomic.Int64) time.Duration {
	const base = 100 * time.Millisecond
	if strategy == "raw" {
		wait := 10 * time.Millisecond // effectively immediate
		backoffSum.Add(int64(wait / time.Microsecond))
		return wait
	}
	exp := base << min(attempts, 5) // caps exponent growth: 3.2s max before jitter
	if exp > 5*time.Second {
		exp = 5 * time.Second
	}
	// Full jitter: uniform in [0, exp].
	j, _ := rand.Int(rand.Reader, big.NewInt(int64(exp)))
	wait := time.Duration(j.Int64())
	backoffSum.Add(int64(wait / time.Microsecond))
	return wait
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
