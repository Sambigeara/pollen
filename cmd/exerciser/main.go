// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// exerciser drives synthetic load against a pollen node's control API.
//
// The binary is deployed on a dedicated host that does NOT run pollen.
// It targets a specific pollen node via its TCP control endpoint
// (enabled on the node with `pln set control-addr :PORT`), fires
// CallWorkload requests at a configured rate, and exposes Prometheus
// metrics at /metrics for an external scraper.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/control"
)

const (
	defaultRate        = 100
	defaultURI         = "pln://seed/ingest/handle"
	defaultMetricsAddr = ":9192"

	callTimeout              = 30 * time.Second
	metricsReadHeaderTimeout = 10 * time.Second
	metricsShutdownTimeout   = 2 * time.Second

	// inflightHeadroom keeps the semaphore non-zero at rate=0 (pathological)
	// and avoids boundary flakiness when rate × callTimeout rounds low.
	inflightHeadroom = 4
	// shedReasonInflightCap labels sheds caused by the safety cap — the
	// target is slower than the offered rate, not that the client errored.
	shedReasonInflightCap = "inflight_cap"
)

func main() {
	target := flag.String("target", "", "host:port of the target pollen node's TCP control endpoint (required)")
	token := flag.String("token", "", "shared secret for the control endpoint (defaults to $PLN_CONTROL_TOKEN)")
	rate := flag.Int("rate", defaultRate, "target calls per second")
	duration := flag.Duration("duration", 0, "run duration; zero means run until signalled")
	uri := flag.String("uri", defaultURI, "pln:// URI to invoke")
	metricsAddr := flag.String("metrics-addr", defaultMetricsAddr, "address to serve /metrics on (empty to disable)")
	label := flag.String("label", os.Getenv("EXERCISER_TARGET_LABEL"), "value for the 'target' metric label (defaults to $EXERCISER_TARGET_LABEL, else --target)")
	flag.Parse()

	if *target == "" {
		log.Fatal("--target is required")
	}
	if *rate <= 0 {
		log.Fatal("--rate must be positive")
	}
	secret := *token
	if secret == "" {
		secret = os.Getenv("PLN_CONTROL_TOKEN")
	}
	targetLabel := *label
	if targetLabel == "" {
		targetLabel = *target
	}

	reg := prometheus.NewRegistry()
	calls := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "exerciser_calls_total",
		Help: "Calls that actually left the client, by target and outcome. Outcome=ok for successful RPCs, outcome=fail only for RPCs that returned an error or timed out. Client-side drops are not counted here — see exerciser_sheds_total.",
	}, []string{"target", "outcome"})
	sheds := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "exerciser_sheds_total",
		Help: "Ticks that were NOT sent because the client's in-flight safety cap was full. Non-zero means the target is responding slower than the offered rate — not a server failure.",
	}, []string{"target", "reason"})
	callDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "exerciser_call_seconds",
		Help:    "Wall-clock latency of CallWorkload, measured end-to-end at the client.",
		Buckets: prometheus.DefBuckets,
	}, []string{"target"})
	inflight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "exerciser_inflight",
		Help: "Current number of in-flight CallWorkload invocations.",
	}, []string{"target"})
	reg.MustRegister(calls, sheds, callDuration, inflight)
	// Pre-create series so scrapers see zero rather than absent.
	calls.WithLabelValues(targetLabel, "ok")
	calls.WithLabelValues(targetLabel, "fail")
	sheds.WithLabelValues(targetLabel, shedReasonInflightCap)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if stopMetrics := startMetricsServer(*metricsAddr, reg); stopMetrics != nil {
		defer stopMetrics()
	}

	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("dial %s: %v", *target, err)
		return
	}
	defer conn.Close()
	client := controlv1.NewControlServiceClient(conn)

	// Size the in-flight cap at rate × callTimeout. Any call that's still
	// in flight past callTimeout has been cancelled by its context anyway,
	// so sizing for that window lets realistic steady-state in-flight
	// (rate × p95) sit comfortably below the cap without shedding.
	// Shedding should only fire on genuine goroutine runaway.
	maxInflight := (*rate)*int(callTimeout/time.Second) + inflightHeadroom
	sem := semaphore.NewWeighted(int64(maxInflight))

	if *duration > 0 {
		log.Printf("exerciser firing %d/s at %s for %s (uri=%s)", *rate, *target, *duration, *uri)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	} else {
		log.Printf("exerciser firing %d/s at %s (uri=%s)", *rate, *target, *uri)
	}

	runLoadLoop(ctx, client, secret, *uri, *rate, targetLabel, sem, calls, sheds, callDuration, inflight)
}

func runLoadLoop(
	ctx context.Context,
	client controlv1.ControlServiceClient,
	token, uri string,
	rate int,
	label string,
	sem *semaphore.Weighted,
	calls, sheds *prometheus.CounterVec,
	callDuration *prometheus.HistogramVec,
	inflight *prometheus.GaugeVec,
) {
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	okCounter := calls.WithLabelValues(label, "ok")
	failCounter := calls.WithLabelValues(label, "fail")
	shedCounter := sheds.WithLabelValues(label, shedReasonInflightCap)
	durationHist := callDuration.WithLabelValues(label)
	gauge := inflight.WithLabelValues(label)

	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			log.Print("draining in-flight calls...")
			wg.Wait()
			return
		case <-ticker.C:
			if !sem.TryAcquire(1) {
				shedCounter.Inc()
				continue
			}
			wg.Add(1)
			gauge.Inc()
			go func() {
				defer wg.Done()
				defer sem.Release(1)
				defer gauge.Dec()

				callCtx, cancel := context.WithTimeout(context.Background(), callTimeout)
				defer cancel()
				if token != "" {
					callCtx = metadata.AppendToOutgoingContext(callCtx, control.ControlTokenMetadataKey, token)
				}

				start := time.Now()
				input := fmt.Appendf(nil, `{"ts":%d}`, time.Now().UnixMilli())
				_, err := client.CallWorkload(callCtx, &controlv1.CallWorkloadRequest{Uri: uri, Input: input})
				durationHist.Observe(time.Since(start).Seconds())
				if err != nil {
					failCounter.Inc()
				} else {
					okCounter.Inc()
				}
			}()
		}
	}
}

func startMetricsServer(addr string, reg *prometheus.Registry) func() {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: metricsReadHeaderTimeout}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server: %v", err)
		}
	}()
	log.Printf("metrics on %s", addr)
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}
