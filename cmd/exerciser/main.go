// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	minWorkers               = 32
	maxWorkers               = 4096
	inputBufferCap           = 32
	callTimeout              = 30 * time.Second
	metricsReadHeaderTimeout = 10 * time.Second
	metricsShutdownTimeout   = 2 * time.Second

	shedReasonInflightCap = "inflight_cap"
)

func workersFor(rate int) int {
	switch {
	case rate < minWorkers:
		return minWorkers
	case rate > maxWorkers:
		return maxWorkers
	default:
		return rate
	}
}

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

	if *duration > 0 {
		log.Printf("exerciser firing %d/s at %s for %s (uri=%s)", *rate, *target, *duration, *uri)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	} else {
		log.Printf("exerciser firing %d/s at %s (uri=%s)", *rate, *target, *uri)
	}

	runLoadLoop(ctx, client, secret, *uri, *rate, targetLabel, calls, sheds, callDuration, inflight)
}

func runLoadLoop(
	ctx context.Context,
	client controlv1.ControlServiceClient,
	token, uri string,
	rate int,
	label string,
	calls, sheds *prometheus.CounterVec,
	callDuration *prometheus.HistogramVec,
	inflight *prometheus.GaugeVec,
) {
	okCounter := calls.WithLabelValues(label, "ok")
	failCounter := calls.WithLabelValues(label, "fail")
	shedCounter := sheds.WithLabelValues(label, shedReasonInflightCap)
	durationHist := callDuration.WithLabelValues(label)
	gauge := inflight.WithLabelValues(label)

	baseCtx := ctx
	if token != "" {
		baseCtx = metadata.AppendToOutgoingContext(ctx, control.ControlTokenMetadataKey, token)
	}

	n := workersFor(rate)
	ticks := make(chan struct{}, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go worker(baseCtx, client, uri, ticks, gauge, durationHist, okCounter, failCounter, &wg)
	}

	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(ticks)
			log.Print("draining in-flight calls...")
			wg.Wait()
			return
		case <-ticker.C:
			select {
			case ticks <- struct{}{}:
			default:
				shedCounter.Inc()
			}
		}
	}
}

func worker(
	baseCtx context.Context,
	client controlv1.ControlServiceClient,
	uri string,
	ticks <-chan struct{},
	gauge prometheus.Gauge,
	durationHist prometheus.Observer,
	okCounter, failCounter prometheus.Counter,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	req := &controlv1.CallWorkloadRequest{Uri: uri}
	buf := make([]byte, 0, inputBufferCap)
	for range ticks {
		gauge.Inc()
		buf = append(buf[:0], `{"ts":`...)
		buf = strconv.AppendInt(buf, time.Now().UnixMilli(), 10) //nolint:mnd
		buf = append(buf, '}')
		req.Input = buf

		callCtx, cancel := context.WithTimeout(baseCtx, callTimeout)
		start := time.Now()
		_, err := client.CallWorkload(callCtx, req)
		durationHist.Observe(time.Since(start).Seconds())
		cancel()
		if err != nil {
			failCounter.Inc()
		} else {
			okCounter.Inc()
		}
		gauge.Dec()
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
