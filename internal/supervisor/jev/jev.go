package jev

// import (
// 	"context"
// 	"net/http"

// 	"github.com/amangale/bidi-rig/internal/supervisor"
// )

// type Client struct {
// 	http    *http.Client
// 	apiKey  string
// 	breaker *Breaker // simple closed/open/half-open
// }

// func (c *Client) Decide(ctx context.Context, s supervisor.Snapshot) (supervisor.Decision, error) {
// 	if !c.breaker.Ready() {
// 		return supervisor.Decision{}, ErrCircuitOpen // caller falls back
// 	}

// 	req := Request{
// 		Model: "jev-latest",
// 		State: serialize(s), // stable JSON encoding of the snapshot
// 		Questions: map[string]Question{
// 			"action": {
// 				Type: "choice",
// 				Instructions: "Given this gRPC streaming run state under partial outage, " +
// 					"choose the next supervisory action.",
// 				Choices: []string{
// 					"continue", "switch_strategy", "force_reconnect", "abort_run",
// 				},
// 			},
// 			"is_outage": {
// 				Type:         "boolean",
// 				Instructions: "Do these symptoms indicate a sustained outage rather than transient noise?",
// 			},
// 			"severity": {
// 				Type:         "score",
// 				Instructions: "Rate severity of the degradation on this scale.",
// 				Scale:        []string{"healthy", "degraded", "critical", "fatal"},
// 			},
// 		},
// 	}
// 	// POST https://api.typesafe.ai/v1/systemone
// 	// map answers back to Decision{Action, Confidence: dist shape, Raw}
// }
