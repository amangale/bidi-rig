package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	rigv1 "github.com/amangale/bidi-rig/proto/rig/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	target   string
	n        int
	rate     time.Duration
	payload  int
	strategy string // "raw" | "jittered"
	duration time.Duration
}

// Global tallies — client-side evidence for F2 (storm timing),
// F5 (duplicates/gaps after reconnect).
type totals struct {
	sent       atomic.Uint64
	acked      atomic.Uint64
	duplicates atomic.Uint64
	reconnects atomic.Uint64
	backoffSum atomic.Int64 // microseconds spent in backoff (jittered vs raw)
}

func main() {
	var (
		target   = flag.String("target", "localhost:50051", "server target")
		n        = flag.Int("clients", 10, "number of simulated clients")
		rateMs   = flag.Int("rate-ms", 100, "ping interval per client")
		payload  = flag.Int("payload", 0, "extra payload bytes per ping (F4)")
		strategy = flag.String("backoff", "raw", "raw | jittered (F2 toggle)")
		duration = flag.Duration("for", 60*time.Second, "run duration")
	)
	flag.Parse()

	cfg := &config{
		target:   *target,
		n:        *n,
		rate:     time.Duration(*rateMs) * time.Millisecond,
		payload:  *payload,
		strategy: *strategy,
		duration: *duration,
	}

	if cfg.strategy != "raw" && cfg.strategy != "jittered" {
		log.Fatalf("unknown backoff strategy %q (want raw|jittered)", cfg.strategy)
	}

	var t totals
	deadline := time.Now().Add(cfg.duration)

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < cfg.n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runClient(id, cfg, &t, deadline)
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("client summary (%s strategy): sent=%d acked=%d duplicates=%d reconnects=%d "+
		"backoff_time=%s over=%s\n",
		cfg.strategy, t.sent.Load(), t.acked.Load(), t.duplicates.Load(),
		t.reconnects.Load(), time.Duration(t.backoffSum.Load())*time.Microsecond, elapsed)
}

// runClient drives one simulated client across any number of reconnects.
// seq is MONOTONIC ACROSS RECONNECTS — that's what makes F5 detectable:
// a fresh stream does not imply a fresh sequence.
func runClient(id int, cfg *config, t *totals, deadline time.Time) {
	seq := uint64(0)
	seen := make(map[uint64]bool) // ack_seq -> observed; duplicates = re-observation
	var mu sync.Mutex             // guards seen during concurrent recvs (one per connection life)

	conn, err := grpc.NewClient(cfg.target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("client %d: dial: %v\n", id, err)
		return
	}
	defer conn.Close()

	//ctx, cancelF := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	//defer cancelF()

	// for time.Now().Before(deadline) {
	// 	stream, err := rigv1.NewRigClient(conn).Session(ctx)

	// 	if err != nil {
	// 		// Note: use ctx with deadline — see the design note below.
	// 		break
	// 	}
	// 	t.reconnects.Add(1)

	// 	if err := sessionIO(stream, id, cfg, &seq, seen,
	// 		&mu, t, deadline); err != nil {
	// 		// Transport died (SIGKILL, LB cut, etc.) — F2 kicks in here.
	// 		wait := backoff(cfg.strategy, t.reconnects.Load(), &t.backoffSum)
	// 		log.Printf("client %d: stream error (%v) — reconnecting in %s\n",
	// 			id, err, wait)
	// 		time.Sleep(wait)
	// 	}
	// 	// On clean EOF (serr == nil): stream half-closed by server gracefully.
	// 	if time.Now().After(deadline) {
	// 		break
	// 	}
	// 	log.Printf("client %d: — reconnected\n", id)
	// }
	first := true
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Until(deadline)+2*time.Second)
		stream, err := rigv1.NewRigClient(conn).Session(ctx)
		if err != nil {
			cancel()
			//log.Fatalf("client %d: dial: %v", id, err) // only fatal if FIRST connect
			wait := backoff(cfg.strategy, t.reconnects.Load(), &t.backoffSum)
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
		if err := sessionIO(stream, id, cfg, &seq, seen,
			&mu, t, deadline); err != nil {
			// Transport died (SIGKILL, LB cut, etc.) — F2 kicks in here.
			wait := backoff(cfg.strategy, t.reconnects.Load(), &t.backoffSum)
			log.Printf("client %d: stream error (%v) — reconnecting in %s\n",
				id, err, wait)
			time.Sleep(wait)
		}
		// On clean EOF (serr == nil): stream half-closed by server gracefully.
		if time.Now().After(deadline) {
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
	seen map[uint64]bool, mu *sync.Mutex, t *totals, deadline time.Time) error {

	errCh := make(chan error, 2)
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
	err := <-errCh
	// Best effort: stop the other goroutine by cancelling the stream.
	// Recv/Send will surface errors once the stream tears down.
	_ = err
	return err
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
