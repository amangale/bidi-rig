package supervisor

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrCircuitOpen     = errors.New("circuit breaker is open")
	ErrCircuitHalfOpen = errors.New("circuit in half-open state, probe failed")
)

// Breaker implements a standard three-state circuit breaker
// States: closed (normal) → open (failing) → half-open (probe) → closed/half-open
type Breaker struct {
	failures      atomic.Int64
	state         int64 // 0=closed, 1=open, 2=half-open
	lastFailureTs int64 // unix nanoseconds

	config BreakerConfig

	successProbe atomic.Bool // track half-open probe result
}

type BreakerConfig struct {
	FailureThreshold int64         // failures before opening
	ResetTimeout     time.Duration // time before attempting half-open probe
	ProbeSuccesses   int64         // successes needed to close from half-open
	MaxFailureWindow time.Duration // reset failures if none occur within this window
}

var DefaultBreakerConfig = BreakerConfig{
	FailureThreshold: 5,
	ResetTimeout:     10 * time.Second,
	ProbeSuccesses:   2,
	MaxFailureWindow: 60 * time.Second,
}

func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.MaxFailureWindow == 0 {
		cfg.MaxFailureWindow = DefaultBreakerConfig.MaxFailureWindow
	}
	if cfg.ResetTimeout == 0 {
		cfg.ResetTimeout = DefaultBreakerConfig.ResetTimeout
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = DefaultBreakerConfig.FailureThreshold
	}
	return &Breaker{config: cfg}
}

func (b *Breaker) RecordSuccess() {
	st := atomic.LoadInt64(&b.state)
	switch st {
	case 0: // closed — success resets nothing, we're healthy
		return
	case 2: // half-open — this was our probe
		// atomically increment successes; if we hit threshold, transition to closed
		if atomic.CompareAndSwapInt64(&b.state, 2, 0) {
			atomic.StoreInt64(&b.lastFailureTs, 0)
		}
	case 1: // open — ignore success (we're not listening)
		return
	}
}

func (b *Breaker) RecordFailure() {
	st := atomic.LoadInt64(&b.state)
	now := time.Now().UnixNano()

	if st == 2 {
		// half-open probe failed — stay in half-open, don't increment failure count
		b.successProbe.Store(false)
		return
	}

	failures := b.failures.Add(1)
	atomic.StoreInt64(&b.lastFailureTs, now)

	if failures >= b.config.FailureThreshold {
		b.transitionToOpen()
	}
}

func (b *Breaker) transitionToOpen() {
	atomic.StoreInt64(&b.state, 1) // open
	go func() {
		time.Sleep(b.config.ResetTimeout)
		if atomic.LoadInt64(&b.state) == 1 {
			b.transitionToHalfOpen()
		}
	}()
}

func (b *Breaker) transitionToHalfOpen() {
	b.failures.Store(0)
	atomic.StoreInt64(&b.lastFailureTs, 0)
	b.successProbe.Store(true)
	atomic.StoreInt64(&b.state, 2)
}

func (b *Breaker) Ready() bool {
	st := atomic.LoadInt64(&b.state)
	switch st {
	case 0: // closed — all good
		return true
	case 2: // half-open — one chance to probe
		return b.successProbe.Load()
	case 1: // open — check if we've exceeded reset timeout
		return b.shouldAttemptRecovery()
	default:
		return false
	}
}

func (b *Breaker) shouldAttemptRecovery() bool {
	lastFail := atomic.LoadInt64(&b.lastFailureTs)
	if lastFail == 0 {
		return true
	}
	now := time.Now().UnixNano()
	duration := time.Duration(now - lastFail)
	return duration >= b.config.ResetTimeout
}

func (b *Breaker) State() string {
	switch atomic.LoadInt64(&b.state) {
	case 0:
		return "closed"
	case 1:
		return "open"
	case 2:
		return "half-open"
	default:
		return "unknown"
	}
}

// ForceOpen and ForceClosed are for testing only
func (b *Breaker) ForceOpen() { atomic.StoreInt64(&b.state, 1) }
func (b *Breaker) ForceClosed() {
	atomic.StoreInt64(&b.state, 0)
	b.failures.Store(0)
}
