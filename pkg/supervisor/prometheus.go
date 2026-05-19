// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

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
	debugPprofEnvVar            = "PLN_DEBUG_PPROF"

	// Must match placement.callTrackerWindow.
	callCountsWindowSeconds = 30
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

	n.spawn(func() {
		<-ctx.Done()
		srv.Close() //nolint:errcheck
	})

	n.log.Infow("prometheus server listening", "addr", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type stateCollector struct {
	snapshot func() state.Snapshot

	nodeInfo         *prometheus.Desc
	nodeCPU          *prometheus.Desc
	nodeMem          *prometheus.Desc
	nodeVivaldiError *prometheus.Desc
	trafficIn        *prometheus.Desc
	trafficOut       *prometheus.Desc
	workloadInfo     *prometheus.Desc
	workloadReplicas *prometheus.Desc
	workloadClaim    *prometheus.Desc
	workloadLoad     *prometheus.Desc
}

func newStateCollector(snapshot func() state.Snapshot) *stateCollector {
	return &stateCollector{
		snapshot:         snapshot,
		nodeInfo:         prometheus.NewDesc("pollen_node_info", "Node presence in the cluster.", []string{"name", "peer"}, nil),
		nodeCPU:          prometheus.NewDesc("pollen_node_cpu_percent", "Node CPU utilisation.", []string{"name"}, nil),
		nodeMem:          prometheus.NewDesc("pollen_node_mem_percent", "Node memory utilisation.", []string{"name"}, nil),
		nodeVivaldiError: prometheus.NewDesc("pollen_node_vivaldi_error", "Node Vivaldi coordinate error estimate (0-1, lower = more confident). High cluster-wide values indicate unconverged coordinates.", []string{"name"}, nil),
		trafficIn:        prometheus.NewDesc("pollen_traffic_rate_in_bytes", "Inbound traffic rate between peers.", []string{"node", "peer"}, nil),
		trafficOut:       prometheus.NewDesc("pollen_traffic_rate_out_bytes", "Outbound traffic rate between peers.", []string{"node", "peer"}, nil),
		workloadInfo:     prometheus.NewDesc("pollen_workload_info", "Workload presence in the cluster.", []string{"name", "hash", "publisher"}, nil),
		workloadReplicas: prometheus.NewDesc("pollen_workload_replicas", "Active replica count for a workload.", []string{"name"}, nil),
		workloadClaim:    prometheus.NewDesc("pollen_workload_claim", "Workload claimed by a node.", []string{"workload", "node"}, nil),
		workloadLoad:     prometheus.NewDesc("pollen_workload_load", "Calls/sec a node has made against a workload over its most recent gossip window.", []string{"workload", "node"}, nil),
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
	ch <- c.workloadLoad
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

	// Replica/claim/load are content-addressed: one running module per
	// hash, claimants shared across every publisher of identical bytes.
	// They stay keyed by the deduped Specs view.
	workloadNames := make(map[string]string, len(snap.Specs))
	for hash, spec := range snap.Specs {
		name := spec.Spec.Name
		if name == "" {
			name = hash[:8] //nolint:mnd
		}
		workloadNames[hash] = name
		claimants := snap.Claims[hash]
		ch <- prometheus.MustNewConstMetric(c.workloadReplicas, prometheus.GaugeValue, float64(len(claimants)), name)

		for claimant := range claimants {
			ch <- prometheus.MustNewConstMetric(c.workloadClaim, prometheus.GaugeValue, 1, name, nodeNames[claimant])
		}
	}

	// workloadInfo is the publication-identity plane: one series per
	// (authority, name) over the un-deduped source, so byte-identical
	// workloads from different tenants are each visible rather than
	// collapsing to a single content-hash entry.
	for _, sv := range snap.SpecsAll {
		name := sv.Spec.Name
		if name == "" {
			name = sv.Spec.Hash[:8] //nolint:mnd
		}
		ch <- prometheus.MustNewConstMetric(c.workloadInfo, prometheus.GaugeValue, 1, name, sv.Spec.Hash[:8], sv.Publisher.Short()) //nolint:mnd
	}

	for pk, nv := range snap.Nodes {
		nodeName := nodeNames[pk]
		for hash, count := range nv.CallCounts {
			workload, ok := workloadNames[hash]
			if !ok {
				workload = hash[:8] //nolint:mnd
			}
			rps := float64(count) / float64(callCountsWindowSeconds)
			ch <- prometheus.MustNewConstMetric(c.workloadLoad, prometheus.GaugeValue, rps, workload, nodeName)
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
