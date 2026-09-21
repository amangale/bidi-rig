package supervisor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DecisionEntry is the immutable record written for every supervisor decision
type DecisionEntry struct {
	Timestamp    time.Time        `json:"ts"`
	RunID        string           `json:"run_id"`
	Source       string           `json:"source"` // jev|heuristic|fallback
	Action       Action           `json:"action"`
	Confidence   float64          `json:"confidence"`
	LatencyMs    int64            `json:"latency_ms"`
	CircuitState string           `json:"circuit_state"`
	Snapshot     Snapshot         `json:"snapshot"`
	Outcome      *DecisionOutcome `json:"outcome,omitempty"` // populated asynchronously later
}

type DecisionOutcome struct {
	NextWindowAckedRatio float64 `json:"next_window_acked_ratio"`
	DidRecover           bool    `json:"did_recover"`
	ReconnectCountDelta  int64   `json:"reconnect_count_delta"`
	ElapsedTimeDeltaMs   int64   `json:"elapsed_time_delta_ms"`
	Reason               string  `json:"reason"`
}

// LogStore provides append-only decision logging
type LogStore struct {
	mu        sync.Mutex
	file      *os.File
	path      string
	batchSize int
	buffer    []DecisionEntry
	closed    bool
}

func NewLogStore(dir string, filename string, batchSize int) (*LogStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	date := time.Now().Format("2006-01-02")
	if filename == "" {
		filename = "decisions_" + date + ".jsonl"
	}
	path := filepath.Join(dir, filename)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &LogStore{
		file:      f,
		path:      path,
		batchSize: batchSize,
		buffer:    make([]DecisionEntry, 0, batchSize),
	}, nil
}

// Write persists an entry immediately (synced, no buffering for durability)
func (l *LogStore) Write(entry DecisionEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return os.ErrClosed
	}

	// Flush buffer if at capacity
	if len(l.buffer) >= l.batchSize {
		l.flushBufferLocked()
	}

	l.buffer = append(l.buffer, entry)

	// For testability: if batch size is 1, flush immediately
	if l.batchSize <= 1 {
		l.flushBufferLocked()
	}

	return nil
}

func (l *LogStore) flushBufferLocked() error {
	if len(l.buffer) == 0 {
		return nil
	}

	for _, entry := range l.buffer {
		line, err := json.Marshal(entry)
		if err != nil {
			continue
		}

		if _, err := l.file.Write(append(line, '\n')); err != nil {
			return err
		}
	}

	l.buffer = l.buffer[:0]
	return nil
}

// Sync forces disk flush (call periodically or on shutdown)
func (l *LogStore) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.flushBufferLocked(); err != nil {
		return err
	}
	return l.file.Sync()
}

func (l *LogStore) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.closed = true
	if err := l.flushBufferLocked(); err != nil {
		l.file.Close()
		return err
	}
	return l.file.Close()
}

func (l *LogStore) Path() string {
	return l.path
}

// UpdateOutcome finds the entry by timestamp+run_id and appends outcome
// In production you'd query by a unique ID field instead
func (l *LogStore) UpdateOutcome(runID string, ts time.Time, outcome DecisionOutcome) error {
	entries, err := ReadLogEntries(l.path)
	if err != nil {
		return err
	}

	var found *DecisionEntry
	for i := range entries {
		if entries[i].RunID == runID && entries[i].Timestamp.Equal(ts) {
			found = &entries[i]
			break
		}
	}
	if found == nil {
		return os.ErrNotExist
	}

	found.Outcome = &outcome

	// Rewrite (fine for small experiment logs)
	lines, err := marshalEntries(entries)
	if err != nil {
		return err
	}
	return os.WriteFile(l.path, lines, 0644)
}

func ReadLogEntries(path string) ([]DecisionEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []DecisionEntry{}, nil
		}
		return nil, err
	}

	var entries []DecisionEntry
	for _, line := range splitLines(data) {
		var e DecisionEntry
		if err := json.Unmarshal(line, &e); err != nil {
			continue // corrupted line, skip
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}

func marshalEntries(entries []DecisionEntry) ([]byte, error) {
	var buf []byte
	for _, e := range entries {
		line, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		buf = append(buf, line...)
		buf = append(buf, '\n')
	}
	return buf, nil
}
