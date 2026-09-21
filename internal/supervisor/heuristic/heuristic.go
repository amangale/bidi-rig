package heuristic

import (
	"context"
	"sync"
	"time"

	"github.com/amangale/bidi-rig/internal/supervisor"
)

type Config struct {
	MinAckedRatio         float64
	ReconnectSoftLimit    int64
	ReconnectHardLimit    int64
	MaxDeadlineExceeded   int64
	BackoffBudgetMs       int64
	CooldownPerAction     time.Duration
	ConsecutiveBadWindows int
}

func DefaultConfig() Config {
	return Config{
		MinAckedRatio:         0.80,
		ReconnectSoftLimit:    3,
		ReconnectHardLimit:    8,
		MaxDeadlineExceeded:   5,
		BackoffBudgetMs:       1500,
		CooldownPerAction:     3 * time.Second,
		ConsecutiveBadWindows: 3,
	}
}

type Supervisor struct {
	cfg Config

	badWindows        int
	lastActionAt      map[supervisor.Action]time.Time
	mu                sync.Mutex
	prevReconnects    int64
	prevBackoffMs     int64
	backoffThisWindow int64
}

func New(cfg Config) *Supervisor {
	return &Supervisor{
		cfg:          cfg,
		lastActionAt: make(map[supervisor.Action]time.Time),
	}
}

func (s *Supervisor) Decide(ctx context.Context, snap supervisor.Snapshot) (supervisor.Decision, error) {
	action, confidence := s.evaluate(snap)

	if s.onCooldown(action) {
		action = supervisor.ActionContinue
		confidence = 0.5
	}

	if action != supervisor.ActionContinue {
		s.mu.Lock()
		s.lastActionAt[action] = time.Now()
		s.mu.Unlock()
	}

	return supervisor.Decision{
		Action:     action,
		Confidence: confidence,
		Source:     "heuristic",
		LatencyMs:  0,
	}, nil
}

func (s *Supervisor) evaluate0(snap supervisor.Snapshot) (supervisor.Action, float64) {
	isBad := snap.AckedRatio < s.cfg.MinAckedRatio

	// Check if THIS tick would complete the threshold (before incrementing)
	if isBad && (s.badWindows+1) >= s.cfg.ConsecutiveBadWindows {
		s.badWindows++ // count this one
		return supervisor.ActionForceReconnect, 0.85
	}

	switch {
	case snap.DeadlineExceeded >= s.cfg.MaxDeadlineExceeded:
		return supervisor.ActionAbortRun, 1.0

	case snap.ReconnectCount >= s.cfg.ReconnectHardLimit:
		return supervisor.ActionAbortRun, 1.0

	case s.risingBackoffRate(snap):
		return supervisor.ActionSwitchStrategy, 0.9

	case snap.ReconnectCount >= s.cfg.ReconnectSoftLimit && s.backoffBurned(snap):
		return supervisor.ActionSwitchStrategy, 0.75

	default:
		// Update counter AFTER decision to track current tick state
		s.mu.Lock()
		if isBad {
			s.badWindows++
		} else {
			s.badWindows = 0
		}
		s.mu.Unlock()

		return supervisor.ActionContinue, 0.95
	}
}

func (s *Supervisor) evaluate(snap supervisor.Snapshot) (supervisor.Action, float64) {
	// Streak update is unconditional and happens FIRST:
	// this tick is evidence regardless of which rule (if any) fires.
	if snap.AckedRatio < s.cfg.MinAckedRatio {
		s.badWindows++
	} else {
		s.badWindows = 0
	}

	switch {
	case snap.DeadlineExceeded >= s.cfg.MaxDeadlineExceeded:
		return supervisor.ActionAbortRun, 1.0

	case snap.ReconnectCount >= s.cfg.ReconnectHardLimit:
		return supervisor.ActionAbortRun, 1.0

	case s.risingBackoffRate(snap):
		return supervisor.ActionSwitchStrategy, 0.9

	case snap.ReconnectCount >= s.cfg.ReconnectSoftLimit && s.backoffBurned(snap):
		return supervisor.ActionSwitchStrategy, 0.75

	case s.badWindows >= s.cfg.ConsecutiveBadWindows:
		return supervisor.ActionForceReconnect, 0.85

	default:
		return supervisor.ActionContinue, 0.95
	}
}

func (s *Supervisor) onCooldown(a supervisor.Action) bool {
	if a == supervisor.ActionContinue {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	last, ok := s.lastActionAt[a]
	if !ok {
		return false
	}
	return time.Since(last) < s.cfg.CooldownPerAction
}

func (s *Supervisor) risingBackoffRate(snap supervisor.Snapshot) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	deltaBackoff := snap.BackoffDurationMs - s.prevBackoffMs
	s.prevBackoffMs = snap.BackoffDurationMs

	deltaReconnects := snap.ReconnectCount - s.prevReconnects
	s.prevReconnects = snap.ReconnectCount

	s.backoffThisWindow += deltaBackoff

	if deltaReconnects > 0 && s.backoffThisWindow > s.cfg.BackoffBudgetMs {
		s.backoffThisWindow = 0
		return true
	}
	return false
}

func (s *Supervisor) backoffBurned(snap supervisor.Snapshot) bool {
	return snap.BackoffDurationMs >= s.cfg.BackoffBudgetMs
}
