package supervisor

import (
	"context"
	"errors"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	prometheusReadHeaderTimeout = 10 * time.Second
	// debugPprofEnvVar gates exposure of net/http/pprof endpoints on the
	// metrics HTTP server. Off by default — pprof leaks goroutine stacks,
	// heap contents, and can be used as a DoS vector.
	debugPprofEnvVar = "PLN_DEBUG_PPROF"
)

func (n *Supervisor) startPrometheus(ctx context.Context, addr string) error {
	n.promRegistry.MustRegister(newStateCollector(n.store.Snapshot))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(n.promRegistry, promhttp.HandlerOpts{}))
	if os.Getenv(debugPprofEnvVar) != "" {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		n.log.Infow("pprof handlers enabled via env var", "var", debugPprofEnvVar)
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: prometheusReadHeaderTimeout,
	}

	go func() {
		<-ctx.Done()
		srv.Close() //nolint:errcheck
	}()

	n.log.Infow("prometheus server listening", "addr", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// stateCollector implements prometheus.Collector by reading the current gossip
// state snapshot on each scrape and emitting node, workload, and traffic metrics.
type stateCollector struct {
	snapshot func() state.Snapshot

	nodeInfo            *prometheus.Desc
	nodeCPU             *prometheus.Desc
	nodeMem             *prometheus.Desc
	nodeVivaldiError    *prometheus.Desc
	trafficIn           *prometheus.Desc
	trafficOut          *prometheus.Desc
	workloadInfo        *prometheus.Desc
	workloadReplicas    *prometheus.Desc
	workloadClaim       *prometheus.Desc
	workloadComputeCost *prometheus.Desc
	workloadParkedMs    *prometheus.Desc
	workloadLoad        *prometheus.Desc
	workloadGateWait    *prometheus.Desc
	workloadBurnRatio   *prometheus.Desc
}

func newStateCollector(snapshot func() state.Snapshot) *stateCollector {
	return &stateCollector{
		snapshot:            snapshot,
		nodeInfo:            prometheus.NewDesc("pollen_node_info", "Node presence in the cluster.", []string{"name", "peer"}, nil),
		nodeCPU:             prometheus.NewDesc("pollen_node_cpu_percent", "Node CPU utilisation.", []string{"name"}, nil),
		nodeMem:             prometheus.NewDesc("pollen_node_mem_percent", "Node memory utilisation.", []string{"name"}, nil),
		nodeVivaldiError:    prometheus.NewDesc("pollen_node_vivaldi_error", "Node Vivaldi coordinate error estimate (0-1, lower = more confident). High cluster-wide values indicate unconverged coordinates.", []string{"name"}, nil),
		trafficIn:           prometheus.NewDesc("pollen_traffic_rate_in_bytes", "Inbound traffic rate between peers.", []string{"node", "peer"}, nil),
		trafficOut:          prometheus.NewDesc("pollen_traffic_rate_out_bytes", "Outbound traffic rate between peers.", []string{"node", "peer"}, nil),
		workloadInfo:        prometheus.NewDesc("pollen_workload_info", "Workload presence in the cluster.", []string{"name", "hash"}, nil),
		workloadReplicas:    prometheus.NewDesc("pollen_workload_replicas", "Active replica count for a workload.", []string{"name"}, nil),
		workloadClaim:       prometheus.NewDesc("pollen_workload_claim", "Workload claimed by a node.", []string{"workload", "node"}, nil),
		workloadComputeCost: prometheus.NewDesc("pollen_workload_compute_cost_ms", "Per-node EWMA mean wall-time (milliseconds) of a single workload invocation. Mean only — not a histogram, so no p50/p99.", []string{"workload", "node"}, nil),
		workloadParkedMs:    prometheus.NewDesc("pollen_workload_parked_ms", "Per-node EWMA mean time (milliseconds) each invocation spends parked inside pollen_request waiting for downstream responses. Subtract from compute_cost_ms to get active CPU time; drives adaptive gate sizing.", []string{"workload", "node"}, nil),
		workloadLoad:        prometheus.NewDesc("pollen_workload_load", "Per-node served request rate (req/s) for a workload.", []string{"workload", "node"}, nil),
		workloadGateWait:    prometheus.NewDesc("pollen_workload_gate_wait_ms", "Per-node EWMA of time spent waiting for the workload's concurrency gate. High values indicate that node is concurrency-starved for that workload.", []string{"workload", "node"}, nil),
		workloadBurnRatio:   prometheus.NewDesc("pollen_workload_slo_burn_ratio", "Cluster-wide share of recent caller-perspective invocations exceeding the workload's spec'd latency SLO. Aggregates per-node satisfied/burned rates. Drives autoscaling.", []string{"workload"}, nil),
	}
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.nodeInfo
	ch <- c.nodeCPU
	ch <- c.nodeMem
	ch <- c.nodeVivaldiError
	ch <- c.trafficIn
	ch <- c.trafficOut
	ch <- c.workloadInfo
	ch <- c.workloadReplicas
	ch <- c.workloadClaim
	ch <- c.workloadComputeCost
	ch <- c.workloadParkedMs
	ch <- c.workloadLoad
	ch <- c.workloadGateWait
	ch <- c.workloadBurnRatio
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.snapshot()
	nodeNames := resolveNodeNames(snap)

	for pk, nv := range snap.Nodes {
		name := nodeNames[pk]

		ch <- prometheus.MustNewConstMetric(c.nodeInfo, prometheus.GaugeValue, 1, name, pk.Short())
		ch <- prometheus.MustNewConstMetric(c.nodeCPU, prometheus.GaugeValue, float64(nv.CPUPercent), name)
		ch <- prometheus.MustNewConstMetric(c.nodeMem, prometheus.GaugeValue, float64(nv.MemPercent), name)
		ch <- prometheus.MustNewConstMetric(c.nodeVivaldiError, prometheus.GaugeValue, nv.VivaldiErr, name)

		for remotePK, tr := range nv.TrafficRates {
			remoteName := nodeNames[remotePK]
			ch <- prometheus.MustNewConstMetric(c.trafficIn, prometheus.GaugeValue, float64(tr.RateIn), name, remoteName)
			ch <- prometheus.MustNewConstMetric(c.trafficOut, prometheus.GaugeValue, float64(tr.RateOut), name, remoteName)
		}
	}

	for hash, spec := range snap.Specs {
		name := spec.Spec.Name
		if name == "" {
			name = hash[:8] //nolint:mnd
		}
		claimants := snap.Claims[hash]
		ch <- prometheus.MustNewConstMetric(c.workloadInfo, prometheus.GaugeValue, 1, name, hash[:8]) //nolint:mnd
		ch <- prometheus.MustNewConstMetric(c.workloadReplicas, prometheus.GaugeValue, float64(len(claimants)), name)

		for claimant := range claimants {
			ch <- prometheus.MustNewConstMetric(c.workloadClaim, prometheus.GaugeValue, 1, name, nodeNames[claimant])
		}

		var satTotal, burnTotal float64
		for pk, nv := range snap.Nodes {
			m, ok := nv.SeedMetrics[hash]
			if !ok {
				continue
			}
			if m.ComputeCostMs > 0 {
				ch <- prometheus.MustNewConstMetric(c.workloadComputeCost, prometheus.GaugeValue, float64(m.ComputeCostMs), name, nodeNames[pk])
			}
			if m.ParkedMs > 0 {
				ch <- prometheus.MustNewConstMetric(c.workloadParkedMs, prometheus.GaugeValue, float64(m.ParkedMs), name, nodeNames[pk])
			}
			if m.ServedRate > 0 {
				ch <- prometheus.MustNewConstMetric(c.workloadLoad, prometheus.GaugeValue, float64(m.ServedRate), name, nodeNames[pk])
			}
			// Emit gate wait whenever this hash has a SeedMetrics entry,
			// including zero. A nonzero→zero transition is a material
			// clear; omission would pin the Prometheus series at the last
			// scrape with data.
			ch <- prometheus.MustNewConstMetric(c.workloadGateWait, prometheus.GaugeValue, float64(m.GateWaitMs), name, nodeNames[pk])
			satTotal += float64(m.SLOSatisfiedRate)
			burnTotal += float64(m.SLOBurnedRate)
		}

		// Cluster-wide SLO burn ratio. Aggregate by summing
		// per-node satisfied/burned rates before dividing, so a
		// hot region's burn isn't drowned out by healthy regions'
		// satisfied counts in a naive-average.
		if total := satTotal + burnTotal; total > 0 {
			ch <- prometheus.MustNewConstMetric(c.workloadBurnRatio, prometheus.GaugeValue, burnTotal/total, name)
		}
	}
}

func resolveNodeNames(snap state.Snapshot) map[types.PeerKey]string {
	names := make(map[types.PeerKey]string, len(snap.Nodes))
	for pk, nv := range snap.Nodes {
		if nv.Name != "" {
			names[pk] = nv.Name
		} else {
			names[pk] = pk.Short()
		}
	}
	return names
}
