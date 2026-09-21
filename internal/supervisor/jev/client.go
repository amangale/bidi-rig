package jev

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/amangale/bidi-rig/internal/supervisor"
)

// Client wraps the TypeSafe/Vercel API with circuit breaker protection
type Client struct {
	httpClient *http.Client
	apiKey     string
	baseURL    string
	breaker    *supervisor.Breaker
	timeout    time.Duration
}

// NewClient creates a Jev-supervisor client with circuit breaker integration
func NewClient(apiKey, baseURL string, breaker *supervisor.Breaker, timeout time.Duration) *Client {
	if baseURL == "" {
		baseURL = "https://api.typesafe.ai/v1/systemone"
	}
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		apiKey:  apiKey,
		baseURL: baseURL,
		breaker: breaker,
		timeout: timeout,
	}
}

// Questions defines the Jev question schema
type Question struct {
	Type         string   `json:"type"`
	Instructions string   `json:"instructions"`
	Choices      []string `json:"choices,omitempty"`
	Scale        []string `json:"scale,omitempty"`
}

// Request is the Jev systemone API payload
type Request struct {
	Model     string              `json:"model"`
	State     string              `json:"state"`
	Questions map[string]Question `json:"questions"`
}

// Response is the Jev systemone API response
type Response struct {
	Answers map[string]Answer `json:"answers"`
}

type Answer struct {
	Value       interface{} `json:"value"`
	Probability float64     `json:"probability"`
	Confidence  float64     `json:"confidence,omitempty"`
}

// Decide calls Jev with the current snapshot and returns a Decision
func (c *Client) Decide(ctx context.Context, snap supervisor.Snapshot) (supervisor.Decision, error) {
	// Circuit breaker check — if open, caller will fall back to heuristic
	if !c.breaker.Ready() {
		return supervisor.Decision{}, supervisor.ErrCircuitOpen
	}

	start := time.Now()

	// Serialize snapshot to stable JSON state string
	stateJSON, err := json.Marshal(snap)
	if err != nil {
		c.breaker.RecordFailure()
		return supervisor.Decision{}, fmt.Errorf("serialize state: %w", err)
	}

	// Define questions — these define your decision space
	req := Request{
		Model: "jev-latest",
		State: string(stateJSON),
		Questions: map[string]Question{
			"action": {
				Type:         "choice",
				Instructions: "Given this gRPC streaming run state under partial outage, choose the NEXT supervisory action. Options: continue (keep running), switch_strategy (change delivery semantics), force_reconnect (re-establish stream immediately), abort_run (stop this test run).",
				Choices: []string{
					"continue", "switch_strategy", "force_reconnect", "abort_run",
				},
			},
			"is_outage": {
				Type:         "boolean",
				Instructions: "Do these symptoms indicate a sustained outage rather than transient network noise?",
			},
			"severity": {
				Type:         "score",
				Instructions: "Rate severity of degradation. Scale: healthy (good throughput), degraded (minor issues), critical (significant failures), fatal (unrecoverable).",
				Scale:        []string{"healthy", "degraded", "critical", "fatal"},
			},
		},
	}

	payload, err := json.Marshal(req)
	if err != nil {
		c.breaker.RecordFailure()
		return supervisor.Decision{}, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL, bytes.NewReader(payload))
	if err != nil {
		c.breaker.RecordFailure()
		return supervisor.Decision{}, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		c.breaker.RecordFailure()
		return supervisor.Decision{}, fmt.Errorf("HTTP call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		c.breaker.RecordFailure()
		return supervisor.Decision{}, fmt.Errorf("Jev API error %d", resp.StatusCode)
	}

	var jevResp Response
	if err := json.NewDecoder(resp.Body).Decode(&jevResp); err != nil {
		c.breaker.RecordFailure()
		return supervisor.Decision{}, fmt.Errorf("decode response: %w", err)
	}

	c.breaker.RecordSuccess()
	return c.mapToDecision(jevResp, start)
}

func (c *Client) mapToDecision(resp Response, start time.Time) (supervisor.Decision, error) {
	actionVal, ok := resp.Answers["action"]
	if !ok {
		return supervisor.Decision{}, fmt.Errorf("missing action answer")
	}

	actionStr, ok := actionVal.Value.(string)
	if !ok {
		return supervisor.Decision{}, fmt.Errorf("action value not string: %T", actionVal.Value)
	}

	action, err := parseAction(actionStr)
	if err != nil {
		return supervisor.Decision{}, err
	}

	confidence := actionVal.Confidence
	if confidence == 0 {
		// Fallback: derive from probability (some versions use probability field)
		confidence = actionVal.Probability
	}

	return supervisor.Decision{
		Action:     action,
		Confidence: confidence,
		Source:     "jev",
		LatencyMs:  int64(time.Since(start).Milliseconds()),
		Raw:        c.extractProbabilities(resp),
	}, nil
}

func parseAction(s string) (supervisor.Action, error) {
	switch s {
	case "continue", "switch_strategy", "force_reconnect", "abort_run":
		return supervisor.Action(s), nil
	default:
		return supervisor.ActionContinue, fmt.Errorf("unknown action %q", s)
	}
}

func (c *Client) extractProbabilities(resp Response) map[string]float64 {
	probs := make(map[string]float64)
	for k, a := range resp.Answers {
		if a.Probability > 0 {
			probs[k] = a.Probability
		}
		if a.Confidence > 0 {
			probs[k+"_conf"] = a.Confidence
		}
	}
	return probs
}
