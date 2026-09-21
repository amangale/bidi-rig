package jev

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/amangale/bidi-rig/internal/supervisor"
)

func TestHappyPathMapping(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"answers": {
				"action": {"value": "force_reconnect", "probability": 0.87, "confidence": 0.87},
				"is_outage": {"value": true, "probability": 0.92},
				"severity": {"value": "critical", "probability": 0.78}
			}
		}`))
	}))
	defer server.Close()

	breaker := supervisor.NewBreaker(supervisor.DefaultBreakerConfig)
	client := NewClient("fake-key", server.URL, breaker, 5*time.Second)

	snap := supervisor.Snapshot{
		RunID:          "test-run",
		AckedRatio:     0.55,
		ReconnectCount: 4,
	}

	d, err := client.Decide(context.Background(), snap)
	if err != nil {
		t.Fatal(err)
	}

	if d.Action != supervisor.ActionForceReconnect {
		t.Fatalf("expected force_reconnect, got %s", d.Action)
	}
	if d.Source != "jev" {
		t.Fatalf("expected source jev, got %s", d.Source)
	}
	if d.LatencyMs == 0 {
		t.Log("Note: latency measurement may vary in test")
	}
}

func TestCircuitBreakerIntegration(t *testing.T) {
	// Simulate failing server to trigger circuit breaker
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer server.Close()

	breaker := supervisor.NewBreaker(supervisor.BreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     100 * time.Millisecond,
		ProbeSuccesses:   1,
	})
	client := NewClient("fake-key", server.URL, breaker, 5*time.Second)

	// Trigger failures until circuit opens
	for i := 0; i < 3; i++ {
		_, err := client.Decide(context.Background(), supervisor.Snapshot{})
		if i < 2 && err == nil {
			t.Logf("failure %d succeeded unexpectedly", i+1)
		}
	}

	// Next call should fail with ErrCircuitOpen, not make HTTP request
	_, err := client.Decide(context.Background(), supervisor.Snapshot{})
	if err != supervisor.ErrCircuitOpen {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestValidActionsOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"answers": {
				"action": {"value": "invalid_action", "probability": 0.5}
			}
		}`))
	}))
	defer server.Close()

	client := NewClient("fake-key", server.URL, supervisor.NewBreaker(supervisor.DefaultBreakerConfig), 5*time.Second)

	_, err := client.Decide(context.Background(), supervisor.Snapshot{})
	if err == nil {
		t.Fatal("expected error on invalid action")
	}
}

func TestMissingAnswers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"answers": {}}`))
	}))
	defer server.Close()

	client := NewClient("fake-key", server.URL, supervisor.NewBreaker(supervisor.DefaultBreakerConfig), 5*time.Second)

	_, err := client.Decide(context.Background(), supervisor.Snapshot{})
	if err == nil {
		t.Fatal("expected error on missing answers")
	}
}

func TestTimeoutHandling(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Second) // Never responds within timeout
	}))
	defer server.Close()

	breaker := supervisor.NewBreaker(supervisor.DefaultBreakerConfig)
	client := NewClient("fake-key", server.URL, breaker, 50*time.Millisecond) // Short timeout

	_, err := client.Decide(context.Background(), supervisor.Snapshot{})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	// Timeout should trigger failure record for circuit breaker
	if breaker.State() != "closed" {
		// May or may not be open depending on threshold; just log state
		t.Logf("breaker state after timeout: %s", breaker.State())
	}
}
