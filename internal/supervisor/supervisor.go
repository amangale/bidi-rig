package supervisor

import "context"

// Snapshot is derived from pkg/metrics counters + run context.
// Field names double as the JSON that gets serialized into Jev's state string.
type Snapshot struct {
	RunID              string   `json:"run_id"`
	Strategy           string   `json:"strategy"` // raw | idempotent | effective-once
	MessagesSent       int64    `json:"messages_sent"`
	MessagesAcked      int64    `json:"messages_acked"`
	AckedRatio         float64  `json:"acked_ratio"`
	DuplicatesDetected int64    `json:"duplicates_detected"`
	ReconnectCount     int64    `json:"reconnect_count"`
	BackoffDurationMs  int64    `json:"backoff_duration_ms"`
	DeadlineExceeded   int64    `json:"deadline_exceeded_count"`
	ElapsedMs          int64    `json:"elapsed_ms"`
	RecentErrorWindow  []string `json:"recent_errors"` // last N error codes, e.g. Unavailable, DeadlineExceeded
}

type Action string

const (
	ActionContinue       Action = "continue"
	ActionSwitchStrategy Action = "switch_strategy"
	ActionForceReconnect Action = "force_reconnect"
	ActionAbortRun       Action = "abort_run"
)

type Decision struct {
	Action     Action
	Confidence float64
	Source     string // "jev" | "heuristic" | "fallback"  ← critical for the A/B story
	LatencyMs  int64
	Raw        map[string]float64 // full probability distribution per question
}

type Supervisor interface {
	Decide(ctx context.Context, snap Snapshot) (Decision, error)
}
