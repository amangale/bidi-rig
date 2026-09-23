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
	model      string // pinned version (e.g., "jev-1.13.0")
	breaker    *supervisor.Breaker
	timeout    time.Duration
}

// NewClient creates a Jev-supervisor client with circuit breaker integration
func NewClient(apiKey, baseURL string, model string, breaker *supervisor.Breaker, timeout time.Duration) *Client {
	if baseURL == "" {
		baseURL = "https://api.typesafe.ai/v1/systemone"
	}
	if model == "" {
		model = "jev-1.13.0" // pinned to avoid silent drift from "jev-latest"
	}
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	return &Client{
		httpClient: &http.Client{Timeout: timeout},
		apiKey:     apiKey,
		baseURL:    baseURL,
		model:      model,
		breaker:    breaker,
		timeout:    timeout,
	}
}

// Question is one typed question. Criteria is polymorphic per type.
type Question struct {
	Type         string          `json:"type"`
	Instructions string          `json:"instructions"`
	Criteria     json.RawMessage `json:"criteria,omitempty"`
}

// Question constructors keep call sites type-safe
func ChoiceQuestion(instructions string, options map[string]string) Question {
	c, _ := json.Marshal(options)
	return Question{Type: "choice", Instructions: instructions, Criteria: c}
}

func ScoreQuestion(instructions string, levels []string) Question {
	c, _ := json.Marshal(levels)
	return Question{Type: "score", Instructions: instructions, Criteria: c}
}

func NoulQuestion(instructions string) Question {
	return Question{Type: "noul", Instructions: instructions}
}

// Request is the Jev systemone API payload
type Request struct {
	Model     string              `json:"model"`
	State     string              `json:"state"`
	Questions map[string]Question `json:"questions"`
}

// Response is the Jev systemone API response
type Response struct {
	Model   string            `json:"model"`
	Answers map[string]Answer `json:"answers"`
	Usage   struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

type Answer struct {
	Type          string             `json:"type"`
	Choice        string             `json:"choice,omitempty"`
	Noul          float64            `json:"noul,omitempty"`
	Score         float64            `json:"score,omitempty"`
	Confidence    float64            `json:"confidence,omitempty"`
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Legend        map[string]string  `json:"legend,omitempty"`
}

// Decide calls Jev with the current snapshot and returns a Decision
func (c *Client) Decide(ctx context.Context, snap supervisor.Snapshot) (supervisor.Decision, error) {
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

	req := Request{
		Model: c.model,
		State: string(stateJSON),
		Questions: map[string]Question{
			"action": ChoiceQuestion(
				"Given this gRPC streaming run state under partial outage, choose the NEXT supervisory action.",
				map[string]string{
					"continue":        "Keep running the current strategy; state looks acceptable",
					"switch_strategy": "Change delivery semantics (flip raw/jittered backoff behavior)",
					"force_reconnect": "Kill all client streams and reconnect immediately",
					"abort_run":       "Terminate this test run",
				}),
			"is_outage": NoulQuestion(
				"Do these symptoms indicate a sustained outage rather than transient network noise?"),
			"severity": ScoreQuestion(
				"Rate severity of degradation.",
				[]string{"healthy", "degraded", "critical", "fatal"}),
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
		body := make([]byte, 1024)
		n, _ := resp.Body.Read(body)
		return supervisor.Decision{}, fmt.Errorf("Jev API error %d: %s", resp.StatusCode, string(body[:n]))
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
	actionAns, ok := resp.Answers["action"]
	if !ok {
		return supervisor.Decision{}, fmt.Errorf("missing action answer")
	}

	if actionAns.Choice == "" {
		return supervisor.Decision{}, fmt.Errorf("empty choice value in action answer")
	}

	action, err := parseAction(actionAns.Choice)
	if err != nil {
		return supervisor.Decision{}, err
	}

	confidence := actionAns.Confidence
	if confidence == 0 {
		confidence = 1.0 // fallback if confidence missing
	}

	return supervisor.Decision{
		Action:     action,
		Confidence: confidence,
		Source:     "jev",
		LatencyMs:  int64(time.Since(start).Milliseconds()),
		Raw: map[string]float64{
			"choice_confidence": confidence,
			"noul":              resp.Answers["is_outage"].Noul,
			"score":             resp.Answers["severity"].Score,
		},
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
