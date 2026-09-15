package server

import (
	"io"
	"sync"
	"sync/atomic"

	rigv1 "github.com/amangale/bidi-rig/proto/rig/v1"
)

// ExitReason classifies how a stream ended — feeds streams_closed{reason}.
type ExitReason string

const (
	ExitEOF     ExitReason = "eof"     // client half-closed cleanly
	ExitError   ExitReason = "error"   // transport/protocol error
	ExitCtx     ExitReason = "ctx"     // context cancelled (incl. drain timeout races)
	ExitDrained ExitReason = "drained" // stream ended due to server drain
)

// Server is the Rig implementation. All mutable stream state funnels
// through atomics/mutex so the rig itself never produces data races
// (we're demonstrating gRPC failure modes, not Go ones).
type Server struct {
	rigv1.UnimplementedRigServer

	id   string // revealed in every Pong — the pinning detector
	raw  bool   // false => bounded writer semantics
	qcap int    // per-stream send queue capacity in bounded mode

	draining atomic.Bool // set by Drain(); handlers stop admitting work

	// counters
	streamsOpened atomic.Int64
	streamsOpen   atomic.Int64
	msgsOut       atomic.Uint64
	dropped       atomic.Uint64 // bounded-mode overflow count

	closedMu sync.Mutex
	closed   map[ExitReason]*atomic.Int64
}

// New constructs a Server. mode is "raw" or "bounded" (F4 toggle).
func New(id, mode string, qcap int) *Server {
	s := &Server{
		id:     id,
		raw:    mode == "raw",
		qcap:   qcap,
		closed: make(map[ExitReason]*atomic.Int64),
	}
	for _, r := range []ExitReason{ExitEOF, ExitError, ExitCtx, ExitDrained} {
		s.closed[r] = new(atomic.Int64)
	}
	return s
}

func (s *Server) recordClosed(r ExitReason) {
	s.closed[r].Add(1)
}

// Session is the bidirectional stream handler.
//
// Concurrency shape (IMPORTANT — this is where my earlier sketch was wrong):
//   - grpc.ServerStream.Send is NOT safe for concurrent callers.
//   - Therefore: exactly ONE writer goroutine owns SendMsg. The Recv loop
//     only produces Pongs into a queue channel.
//   - F4 lives here: raw mode uses a fat buffer (slow client => unbounded
//     server-side memory). bounded mode uses qcap and drops with a
//     documented policy when full.
func (s *Server) Session(stream rigv1.Rig_SessionServer) error {
	ctx := stream.Context()
	opened := s.streamsOpened.Add(1)
	defer func() {
		s.streamsOpened.Add(-1)
	}()

	// Per-stream outbox. Capacity depends on the mode under test:
	// raw mode: effectively unbounded (oversized buffer) so a slow client
	// grows server memory — the failure we want to measure.
	// bounded mode: small buffer; when full, we apply the bounded policy.
	bufCap := s.qcap
	if s.raw {
		bufCap = 1 << 20 // 1M entries — big enough that it behaves as unbounded
	}
	sendCh := make(chan *rigv1.Pong, bufCap)

	writerDone := make(chan error, 1)

	// THE writer. Sole owner of stream.Send.
	go func() {
		for pong := range sendCh {
			if err := stream.Send(pong); err != nil {
				writerDone <- err
				return
			}
		}
		close(writerDone) // normal shutdown: queue drained
	}()

	// Writer error propagates out of the handler, killing the stream.
	errCh := make(chan error, 1)
	go func() {
		err := <-writerDone
		if err != nil {
			errCh <- err
		}
	}()

	for {
		ping, err := stream.Recv()
		if err == io.EOF {
			// Client half-close: no more inbound. We could drain remaining
			// outbound; for the rig, close the queue and wait briefly.
			close(sendCh)
			<-writerDone
			s.recordClosed(ExitEOF)
			return nil
		}
		if err != nil || ctx.Err() != nil {
			close(sendCh)
			s.recordClosed(ExitError)
			return err
		}

		// Drop policy: bounded mode with full queue => drop oldest (backpressure
		// telemetry instead of unbounded buffering). Change to block/deny and
		// re-measure — that choice IS the F4 experiment.
		select {
		case sendCh <- &rigv1.Pong{
			AckSeq:           ping.Seq,
			ServerId:         s.id,
			StreamOpenUnixMs: opened,
			ConnStreamCount:  uint64(opened),
		}:
			s.msgsOut.Add(1)
		default:
			if s.raw {
				// bounded-mode overflow: count it; observability catches it later
				s.dropped.Add(1)
			} else {
				// raw mode can't get here (buffer is effectively unbounded)
				sendCh <- &rigv1.Pong{
					AckSeq:           ping.GetSeq(),
					ServerId:         s.id,
					StreamOpenUnixMs: opened,
					ConnStreamCount:  uint64(opened),
				}
				s.msgsOut.Add(1)
			}
		}
	}
}

// Drain signals in-flight handlers to conclude and stops new stream admission.
// Full drain semantics (GOAWAY orchestration, budget race) live in Night 3;
// this stub is intentional — main.go compiles against it tonight.
func (s *Server) Drain() {
	s.draining.Store(true)
}
