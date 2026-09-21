package heuristic

import (
	"context"
	"testing"
	"time"

	"github.com/amangale/bidi-rig/internal/supervisor"
)

func baseSnap() supervisor.Snapshot {
	return supervisor.Snapshot{
		RunID:             "test",
		Strategy:          "raw",
		MessagesSent:      1000,
		MessagesAcked:     950,
		AckedRatio:        0.95,
		ReconnectCount:    0,
		BackoffDurationMs: 0,
		DeadlineExceeded:  0,
		ElapsedMs:         5000,
	}
}

func TestHealthyRunContinues(t *testing.T) {
	s := New(DefaultConfig())
	d, err := s.Decide(context.Background(), baseSnap())
	if err != nil {
		t.Fatal(err)
	}
	if d.Action != supervisor.ActionContinue {
		t.Fatalf("expected continue, got %s", d.Action)
	}
	if d.Source != "heuristic" {
		t.Fatalf("expected source heuristic, got %s", d.Source)
	}
}

func TestAbortOnDeadlineExceeded(t *testing.T) {
	s := New(DefaultConfig())
	snap := baseSnap()
	snap.DeadlineExceeded = 5
	d, _ := s.Decide(context.Background(), snap)
	if d.Action != supervisor.ActionAbortRun {
		t.Fatalf("expected abort_run, got %s", d.Action)
	}
}

func TestEscalatingBadWindowsForceReconnect(t *testing.T) {
	s := New(DefaultConfig())
	snap := baseSnap()
	snap.AckedRatio = 0.5

	// Window 1-2: degrading but not yet conclusive
	for i := 0; i < 2; i++ {
		d, _ := s.Decide(context.Background(), snap)
		if d.Action != supervisor.ActionContinue {
			t.Fatalf("window %d: expected continue (waiting for confirmation), got %s", i, d.Action)
		}
	}

	// Window 3: consecutive bad windows tip it over
	d, _ := s.Decide(context.Background(), snap)
	if d.Action != supervisor.ActionForceReconnect {
		t.Fatalf("expected force_reconnect after 3 bad windows, got %s", d.Action)
	}
}

func TestGoodWindowResetsBadCounter(t *testing.T) {
	s := New(DefaultConfig())
	bad := baseSnap()
	bad.AckedRatio = 0.5
	good := baseSnap()
	ctx := context.Background()

	// 2 bad, 1 good (reset), then only 2 consecutive bad:
	// streak is below threshold, must NOT trigger
	s.Decide(ctx, bad)
	s.Decide(ctx, bad)
	s.Decide(ctx, good)
	s.Decide(ctx, bad)
	d, _ := s.Decide(ctx, bad) // 2nd consecutive bad after reset
	if d.Action != supervisor.ActionContinue {
		t.Fatalf("streak of 2 after reset; expected continue, got %s", d.Action)
	}

	// 3rd consecutive bad crosses threshold
	d, _ = s.Decide(ctx, bad)
	if d.Action != supervisor.ActionForceReconnect {
		t.Fatalf("streak of 3; expected force_reconnect, got %s", d.Action)
	}
}

func TestCooldownSuppressesRepeatActions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CooldownPerAction = 10 * time.Second // long enough to stay active in test
	cfg.ReconnectHardLimit = 2
	s := New(cfg)

	snap := baseSnap()
	snap.ReconnectCount = 2

	d1, _ := s.Decide(context.Background(), snap)
	if d1.Action != supervisor.ActionAbortRun {
		t.Fatalf("first decision should abort, got %s", d1.Action)
	}

	snap.ReconnectCount = 3
	d2, _ := s.Decide(context.Background(), snap)
	if d2.Action != supervisor.ActionContinue {
		t.Fatalf("second abort within cooldown should be suppressed to continue, got %s", d2.Action)
	}
}

func TestBackoffRateTriggersStrategySwitch(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BackoffBudgetMs = 100
	s := New(cfg)

	snap := baseSnap()
	snap.ReconnectCount = 2
	snap.BackoffDurationMs = 50
	s.Decide(context.Background(), snap)

	// Next tick: reconnects climbing AND backoff burning budget
	snap.ReconnectCount = 3
	snap.BackoffDurationMs = 200
	d, _ := s.Decide(context.Background(), snap)
	if d.Action != supervisor.ActionSwitchStrategy {
		t.Fatalf("expected switch_strategy on backoff burn, got %s", d.Action)
	}
}
