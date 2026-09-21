package supervisor

import (
	"testing"
	"time"
)

func TestLogStoreWriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewLogStore(tmpDir, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	entry := DecisionEntry{
		Timestamp:  time.Unix(1700000000, 0),
		RunID:      "test-run-001",
		Source:     "heuristic",
		Action:     ActionContinue,
		Confidence: 1.0,
		LatencyMs:  5,
		Snapshot: Snapshot{
			MessagesSent: 100,
			AckedRatio:   0.95,
		},
	}

	if err := store.Write(entry); err != nil {
		t.Fatal(err)
	}

	entries, err := ReadLogEntries(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].RunID != "test-run-001" {
		t.Fatalf("wrong RunID: %s", entries[0].RunID)
	}
}

func TestLogStoreUpdateOutcome(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewLogStore(tmpDir, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	entry := DecisionEntry{
		Timestamp:  time.Unix(1700000000, 0),
		RunID:      "test-run-002",
		Source:     "jev",
		Action:     ActionSwitchStrategy,
		Confidence: 0.87,
		LatencyMs:  143,
		Snapshot: Snapshot{
			MessagesSent: 500,
		},
	}
	if err := store.Write(entry); err != nil {
		t.Fatal(err)
	}

	outcome := DecisionOutcome{
		NextWindowAckedRatio: 0.98,
		DidRecover:           true,
	}
	if err := store.UpdateOutcome("test-run-002", entry.Timestamp, outcome); err != nil {
		t.Fatal(err)
	}

	updated, err := ReadLogEntries(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if updated[0].Outcome == nil {
		t.Fatal("outcome not persisted")
	}
	if !updated[0].Outcome.DidRecover {
		t.Error("recovery flag lost in update")
	}
}
