package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/amangale/bidi-rig/internal/server"
	rigv1 "github.com/amangale/bidi-rig/proto/rig/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	var (
		addr         = flag.String("addr", ":50051", "gRPC listen address")
		metricsAddr  = flag.String("metrics", ":9090", "prometheus metrics addr")
		id           = flag.String("id", hostnameOr("srv-1"), "server id (revealed in every Pong)")
		mode         = flag.String("write-mode", "raw", "raw | bounded")
		queueCap     = flag.Int("send-queue-cap", 16, "per-stream send queue capacity (bounded mode)")
		drainTimeout = flag.Duration("drain-timeout", 30*time.Second, "max wait for in-flight streams on SIGTERM")
	)
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := server.New(*id, *mode, *queueCap)

	gs := grpc.NewServer(
		grpc.MaxConcurrentStreams(1024), // tune me during F2/F3 experiments
	)
	rigv1.RegisterRigServer(gs, srv)
	reflection.Register(gs) // grpcurl friendliness

	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(*metricsAddr, nil) // F4/F2 evidence endpoint

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Printf("serve exit: %v", err)
		}
	}()
	log.Printf("%s listening on %s", *id, *addr)

	<-sig
	log.Printf("signal received: draining (budget %s)", *drainTimeout)
	start := time.Now()

	// F3 experiment: drain removes "stop accepting new streams" quickly,
	// then races existing streams against the budget.
	drainDone := make(chan struct{})
	go func() {
		srv.Drain()       // signal in-flight handlers to finish; see internal/server/drain.go
		gs.GracefulStop() // blocks until streams complete
		close(drainDone)
	}()
	select {
	case <-drainDone:
		log.Printf("clean drain in %s", time.Since(start))
	case <-time.After(*drainTimeout):
		log.Printf("drain budget exceeded — hard stop; expect stream losses")
		gs.Stop()
	}
}

func hostnameOr(fallback string) string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return fallback
	}
	return h
}
