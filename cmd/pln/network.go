// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/types"
)

var errNoSuffixMatch = errors.New("no suffix match")

const configProtocolUDP = "udp"

func configProtocolToProto(p string) statev1.ServiceProtocol {
	if p == configProtocolUDP {
		return statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP
	}
	return statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP
}

func protoToConfigString(p statev1.ServiceProtocol) string {
	if p == statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP {
		return configProtocolUDP
	}
	return ""
}

const (
	hoursPerDay      = 24
	daysPerYear      = 365
	minutesPerHour   = 60
	tablePadding     = 2
	minDisambigPeers = 2
	shortHexLen      = 8
)

func newNetworkCmds() []*cobra.Command {
	statusCmd := &cobra.Command{
		Use:   "status [nodes|services|seeds|static|blobs]",
		Short: "Show network status",
		Long: `Renders an overview of the cluster from this node's perspective:
peers, services, seeds, static sites, and blobs. Optionally restrict
output to a single section.

By default, offline peers and orphaned blobs are filtered. Use
--include-offline to include them and --wide for full peer IDs.`,
		Example: "  pln status\n  pln status seeds --wide\n  pln status nodes --include-offline",
		Args:    cobra.RangeArgs(0, 1),
		RunE:    withEnv(runStatus),
	}
	statusCmd.Flags().Bool("wide", false, "Show full peer IDs and extra details")
	statusCmd.Flags().Bool("include-offline", false, "Include offline nodes, services, and orphaned blobs")

	serveCmd := &cobra.Command{
		Use:   "serve <port> [name]",
		Short: "Expose a local port to the mesh",
		Long: `Registers a local TCP (or UDP, with --udp) port as a mesh service.
Other nodes ` + "`pln connect`" + ` to it by name. Persisted to config.yaml so
the service survives restarts.`,
		Example: "  pln serve 8080 api\n  pln serve 5353 dns --udp",
		Args:    cobra.RangeArgs(1, 2), //nolint:mnd
		RunE:    withEnv(runServe),
	}
	serveCmd.Flags().Bool("udp", false, "Expose as a UDP service")
	serveCmd.Flags().StringArray("prop", nil, "Service-publisher property: key=value or JSON object (e.g. --prop '{\"public\":true}')")

	unserveCmd := &cobra.Command{
		Use:   "unserve <port|name>",
		Short: "Stop exposing a local service",
		Long:  "Unregisters a local mesh service by port or name. Removes it from config.yaml so it doesn't come back on restart. Open tunnels from other peers terminate.",
		Args:  cobra.ExactArgs(1),
		RunE:  withEnv(runUnserve),
	}

	connectCmd := &cobra.Command{
		Use:   "connect <service> [provider] [local-port]",
		Short: "Tunnel a local port to a service",
		Long: `Opens a local TCP/UDP listener forwarding to a remote service over
the mesh. If the local port is omitted, the service's remote port is
used. The provider argument disambiguates when multiple peers serve
the same name (use the peer-id prefix shown by ` + "`pln status`" + `).

Persists to config.yaml so the tunnel re-establishes after restart.`,
		Example: "  pln connect api                 # any provider, default port\n  pln connect api ab12 9090       # specific peer + local port",
		Args:    cobra.RangeArgs(1, 3), //nolint:mnd
		RunE:    withEnv(runConnect),
	}

	disconnectCmd := &cobra.Command{
		Use:   "disconnect <service|local-port> [provider]",
		Short: "Close a tunnel to a service",
		Long:  "Tears down a local tunnel and removes it from config.yaml. Identify by service name or local port; pass a provider prefix to disambiguate when multiple tunnels share a name.",
		Args:  cobra.RangeArgs(1, 2), //nolint:mnd
		RunE:  withEnv(runDisconnect),
	}

	denyCmd := &cobra.Command{
		Use:   "deny <peer-id>",
		Short: "Deny a peer's membership",
		Long: `Revokes a peer's cluster membership and tears down its connections.
The peer is denied future re-admission until an admin re-issues credentials.
Identify by hex peer-id prefix (as shown by ` + "`pln status`" + `).`,
		Example: "  pln deny ab12cd34",
		Args:    cobra.ExactArgs(1),
		RunE:    withEnv(runDeny),
	}

	return []*cobra.Command{statusCmd, serveCmd, unserveCmd, connectCmd, disconnectCmd, denyCmd}
}

func runStatus(cmd *cobra.Command, args []string, env *cliEnv) error {
	mode := "all"
	if len(args) == 1 {
		mode = args[0]
	}
	wide, _ := cmd.Flags().GetBool("wide")
	includeAll, _ := cmd.Flags().GetBool("include-offline")
	opts := statusViewOpts{wide: wide, includeAll: includeAll}

	resp, err := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeUnavailable {
			return err
		}
		if socketPermissionDenied(env.dir) {
			return permissionErr("cannot reach daemon — are you in the pln group?\n  fix: sudo usermod -aG pln $(whoami) && newgrp pln")
		}
		return unreachableErr("daemon is not running")
	}

	st := resp.Msg
	metricsResp, metricsErr := env.client.GetMetrics(cmd.Context(), connect.NewRequest(&controlv1.GetMetricsRequest{}))
	var health *controlv1.GetMetricsResponse
	if metricsErr == nil {
		health = metricsResp.Msg
	}
	renderStatusHeader(cmd.OutOrStdout(), health, statusContextLabel(env.dir))

	var sections []statusSection
	switch mode {
	case "all":
		if s := collectPeersSection(st, opts); s.visible() {
			sections = append(sections, s)
		}
		if s := collectServicesSection(st, opts); s.visible() {
			sections = append(sections, s)
		}
		if s := collectSeedsSection(st, opts); s.visible() {
			sections = append(sections, s)
		}
		if s := collectStaticSection(st, opts); s.visible() {
			sections = append(sections, s)
		}
		if s := collectBlobsSection(st, opts); s.visible() {
			sections = append(sections, s)
		}
	case "nodes", "node":
		if s := collectPeersSection(st, opts); s.visible() {
			sections = append(sections, s)
		}
	case "services", "service", "serve":
		if s := collectServicesSection(st, opts); s.visible() {
			sections = append(sections, s)
		}
	case "seeds", "seed":
		if s := collectSeedsSection(st, opts); s.visible() {
			sections = append(sections, s)
		}
	case "static", "sites":
		if s := collectStaticSection(st, opts); s.visible() {
			sections = append(sections, s)
		}
	case "blobs", "blob":
		if s := collectBlobsSection(st, opts); s.visible() {
			sections = append(sections, s)
		}
	default:
		return fmt.Errorf("unknown status selector %q (use: nodes|services|seeds|static|blobs)", mode)
	}

	renderStatusSections(cmd.OutOrStdout(), sections)

	if wide && metricsErr == nil {
		renderMetricsDetails(cmd.OutOrStdout(), metricsResp.Msg)
	}

	renderCertAttributes(cmd.OutOrStdout(), st)

	if footer := certExpiryFooter(st); footer != "" {
		fmt.Fprintln(cmd.OutOrStdout(), footer)
	}

	return nil
}

func runServe(cmd *cobra.Command, args []string, env *cliEnv) error {
	portStr := args[0]
	name := ""
	if len(args) > 1 {
		name = args[1]
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}

	isUDP, _ := cmd.Flags().GetBool("udp")
	proto := statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP
	cfgProto := ""
	if isUDP {
		proto = statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP
		cfgProto = configProtocolUDP
		if name == "" {
			name = portStr + "/" + configProtocolUDP
		}
	}

	props, err := parseProperties(cmd)
	if err != nil {
		return err
	}
	var propMap map[string]any
	if props != nil {
		propMap = props.AsMap()
	}
	env.cfg.AddService(name, uint32(port), cfgProto, propMap)

	sockPath := filepath.Join(env.dir, socketName)
	if nodeSocketActive(sockPath) {
		req := &controlv1.RegisterServiceRequest{Port: uint32(port), Protocol: proto, Properties: props}
		if name != "" {
			req.Name = &name
		}
		if _, err = env.client.RegisterService(cmd.Context(), connect.NewRequest(req)); err != nil {
			return err
		}
	}

	if err := config.Save(env.dir, env.cfg); err != nil {
		return err
	}

	if name != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Registered service %s on port: %s\n", name, portStr)
		return nil
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Registered service on port: %s\n", portStr)
	return nil
}

func runUnserve(cmd *cobra.Command, args []string, env *cliEnv) error {
	arg := args[0]
	var port uint32
	var name string

	if isPortArg(arg) {
		p, _ := strconv.Atoi(arg)
		port = uint32(p)
	} else {
		name = arg
	}

	if name == "" {
		name = strconv.FormatUint(uint64(port), 10)
	}

	resolved, err := resolveServiceByPrefix(env.cfg.Services, name)
	if err != nil {
		return err
	}
	name = resolved
	env.cfg.RemoveService(name)

	sockPath := filepath.Join(env.dir, socketName)
	if nodeSocketActive(sockPath) {
		req := &controlv1.UnregisterServiceRequest{Port: port}
		if name != "" {
			req.Name = &name
		}
		if _, err := env.client.UnregisterService(cmd.Context(), connect.NewRequest(req)); err != nil {
			return err
		}
	}

	if err := config.Save(env.dir, env.cfg); err != nil {
		return err
	}

	if name != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Unregistered service %s\n", name)
		return nil
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Unregistered service on port: %d\n", port)
	return nil
}

func runConnect(cmd *cobra.Command, args []string, env *cliEnv) error {
	serviceArg := args[0]
	var providerArg, localPortArg string
	switch len(args) {
	case 3: //nolint:mnd
		providerArg, localPortArg = args[1], args[2]
	case 2: //nolint:mnd
		if isPortArg(args[1]) {
			localPortArg = args[1]
		} else {
			providerArg = args[1]
		}
	}

	var localPort uint32
	if localPortArg != "" {
		p, err := strconv.Atoi(localPortArg)
		if err != nil || p < minPort || p > maxPort {
			return errors.New("invalid local port")
		}
		localPort = uint32(p)
	}

	statusResp, err := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		return err
	}

	svc, err := resolveService(statusResp.Msg, serviceArg, providerArg)
	if err != nil {
		return err
	}

	connectResp, err := env.client.ConnectService(cmd.Context(), connect.NewRequest(&controlv1.ConnectServiceRequest{
		Node:       &controlv1.NodeRef{PeerPub: svc.GetProvider().GetPeerPub()},
		RemotePort: svc.GetPort(),
		LocalPort:  localPort,
		Protocol:   svc.GetProtocol(),
	}))
	if err != nil {
		return err
	}

	actualLocalPort := connectResp.Msg.GetLocalPort()
	provider := formatPeerID(svc.GetProvider().GetPeerPub(), false)
	peerHex := peerKeyString(svc.GetProvider().GetPeerPub())

	env.cfg.AddConnection(svc.GetName(), peerHex, svc.GetPort(), actualLocalPort, protoToConfigString(svc.GetProtocol()))
	if saveErr := config.Save(env.dir, env.cfg); saveErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to persist connection to config: %v\n", saveErr)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "forwarding localhost:%d -> %s (%s:%d)\n", actualLocalPort, svc.GetName(), provider, svc.GetPort())
	return nil
}

func runDisconnect(cmd *cobra.Command, args []string, env *cliEnv) error {
	arg := args[0]
	var providerArg string
	if len(args) > 1 {
		providerArg = args[1]
	}

	statusResp, err := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		return err
	}

	conn, err := resolveConnection(statusResp.Msg, arg, providerArg)
	if err != nil {
		return err
	}

	if _, err := env.client.DisconnectService(cmd.Context(), connect.NewRequest(&controlv1.DisconnectServiceRequest{
		LocalPort: conn.GetLocalPort(),
	})); err != nil {
		return err
	}

	env.cfg.RemoveConnection(conn.GetLocalPort())
	if saveErr := config.Save(env.dir, env.cfg); saveErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to persist disconnection to config: %v\n", saveErr)
	}

	provider := formatPeerID(conn.GetPeer().GetPeerPub(), false)
	name := conn.GetServiceName()
	if name == "" {
		name = strconv.FormatUint(uint64(conn.GetRemotePort()), 10)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "disconnected localhost:%d from %s (%s:%d)\n",
		conn.GetLocalPort(), name, provider, conn.GetRemotePort())
	return nil
}

func runDeny(cmd *cobra.Command, args []string, env *cliEnv) error {
	prefix := strings.ToLower(args[0])

	statusResp, err := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		return err
	}

	var matches [][]byte
	for _, n := range statusResp.Msg.GetNodes() {
		if peerIDHasPrefix(n.GetNode().GetPeerPub(), prefix) {
			matches = append(matches, n.GetNode().GetPeerPub())
		}
	}

	if len(matches) == 0 {
		return notFoundErr("no peer matching %q", prefix)
	}
	if len(matches) > 1 {
		return ambiguousErr("ambiguous peer prefix %q matches %d peers", prefix, len(matches))
	}

	peerID := matches[0]
	if _, err := env.client.DenyPeer(cmd.Context(), connect.NewRequest(&controlv1.DenyPeerRequest{PeerPub: peerID})); err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "denied peer %s\n", formatPeerID(peerID, false))
	return nil
}

// --- Status View & UI Logic ---

type statusViewOpts struct {
	wide       bool
	includeAll bool
}

type statusSection struct {
	title   string
	footer  string
	headers []string
	rows    [][]string
}

func (s statusSection) visible() bool { return len(s.rows) > 0 || s.footer != "" }

func collectPeersSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "PEERS",
		headers: []string{"NODE", "STATUS", "ADDR", "CPUs", "CPU", "MEM", "TUNNELS", "LATENCY", "IN", "OUT"},
	}

	nameLabels := nodeNameLabels(st.GetSelf(), st.GetNodes(), opts.wide)

	if self := st.GetSelf(); self != nil && self.GetNode() != nil {
		selfPK := peerKeyString(self.GetNode().GetPeerPub())
		label := nameLabels[selfPK]
		if label == "" {
			label = formatPeerID(self.GetNode().GetPeerPub(), opts.wide)
		}
		label += " (self)"
		addr := self.GetAddr()
		if addr == "" {
			addr = "-"
		}
		status := "-"
		if self.GetPubliclyAccessible() {
			status = "- (public)"
		}
		sec.rows = append(sec.rows, []string{
			label, status, addr,
			formatCount(self.GetNumCpu()), formatPercent(self.GetCpuPercent()), formatPercent(self.GetMemPercent()),
			formatCount(self.GetTunnelCount()), "-",
			formatRate(self.GetTrafficRateIn()), formatRate(self.GetTrafficRateOut()),
		})
	}

	filtered := 0
	for _, n := range st.Nodes {
		if !opts.includeAll && !isReachableStatus(n.GetStatus()) {
			filtered++
			continue
		}
		pk := peerKeyString(n.GetNode().GetPeerPub())
		label := nameLabels[pk]
		if label == "" {
			label = formatPeerID(n.GetNode().GetPeerPub(), opts.wide)
		}
		status := formatStatus(n.GetStatus())
		if n.GetPubliclyAccessible() {
			status += " (public)"
		}
		addr := n.GetAddr()
		if addr == "" {
			addr = "-"
		}

		cpus, cpu, mem, tunnels, latency, trafficIn, trafficOut := formatCount(n.GetNumCpu()), formatPercent(n.GetCpuPercent()), formatPercent(n.GetMemPercent()), formatCount(n.GetTunnelCount()), formatLatency(n.GetLatencyMs()), formatRate(n.GetTrafficRateIn()), formatRate(n.GetTrafficRateOut())

		if !isReachableStatus(n.GetStatus()) {
			cpus, cpu, mem, tunnels, latency, trafficIn, trafficOut = "-", "-", "-", "-", "-", "-", "-"
		}

		sec.rows = append(sec.rows, []string{label, status, addr, cpus, cpu, mem, tunnels, latency, trafficIn, trafficOut})
	}

	if filtered > 0 {
		sec.footer = fmt.Sprintf("offline peers: %d (use --include-offline)", filtered)
	}
	return sec
}

func collectServicesSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "SERVICES",
		headers: []string{"SERVICE", "PROVIDER", "PORT", "LOCAL"},
	}

	reachableProviders := reachableProviderSet(st)
	suffixes := nameCollisionSuffixes(st.Services, func(svc *controlv1.ServiceSummary) (string, string, bool) {
		pk := peerKeyString(svc.GetProvider().GetPeerPub())
		return svc.GetName(), pk, opts.includeAll || reachableProviders[pk]
	})
	nameLabels := nodeNameLabels(st.GetSelf(), st.GetNodes(), opts.wide)

	type connKey struct {
		service  string
		provider string
	}
	connLocal := map[connKey]uint32{}
	for _, c := range st.Connections {
		connLocal[connKey{c.GetServiceName(), peerKeyString(c.GetPeer().GetPeerPub())}] = c.GetLocalPort()
	}

	filtered := 0
	hasCollisions := len(suffixes) > 0
	for _, s := range st.Services {
		providerID := peerKeyString(s.GetProvider().GetPeerPub())
		if !opts.includeAll && !reachableProviders[providerID] {
			filtered++
			continue
		}
		provider := nameLabels[providerID]
		if provider == "" {
			provider = formatPeerID(s.GetProvider().GetPeerPub(), opts.wide)
		}
		local := "-"
		if lp, ok := connLocal[connKey{s.GetName(), providerID}]; ok {
			local = fmt.Sprintf("%d", lp)
		}
		displayName := s.GetName()
		if sfx := suffixes[namePeerKey{s.GetName(), providerID}]; sfx != "" {
			displayName += "-" + sfx
		}
		sec.rows = append(sec.rows, []string{displayName, provider, fmt.Sprintf("%d", s.GetPort()), local})
	}

	var footerParts []string
	if hasCollisions {
		footerParts = append(footerParts, "service suffixes match the start of the provider ID")
	}
	if filtered > 0 {
		footerParts = append(footerParts, fmt.Sprintf("offline services: %d (use --include-offline)", filtered))
	}
	sec.footer = strings.Join(footerParts, "\n")
	return sec
}

func collectStaticSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "STATIC",
		headers: []string{"NAME", "MANIFEST", "REPLICAS", "LOCAL", "PUBLISHER"},
	}
	nameLabels := nodeNameLabels(st.GetSelf(), st.GetNodes(), opts.wide)
	for _, site := range st.GetSites() {
		digest := hex.EncodeToString(site.GetManifestDigest())
		if !opts.wide && len(digest) > shortHexLen {
			digest = digest[:shortHexLen]
		}
		target := min(site.GetMinReplicas(), site.GetServingCapacity())
		replicas := fmt.Sprintf("%d/%d", len(site.GetClaimants()), target)
		local := ""
		if site.GetLocal() {
			local = "*"
		}
		publisherPK := peerKeyString(site.GetPublisher().GetPeerPub())
		publisher := nameLabels[publisherPK]
		if publisher == "" {
			publisher = formatPeerID(site.GetPublisher().GetPeerPub(), opts.wide)
		}
		sec.rows = append(sec.rows, []string{site.GetName(), digest, replicas, local, publisher})
	}
	return sec
}

func collectBlobsSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "BLOBS",
		headers: []string{"NAME", "HASH", "REPLICAS", "LOCAL"},
	}
	const defaultBlobLimit = 20
	all := st.GetBlobs()
	var (
		blobs    []*controlv1.BlobSummary
		orphaned int
	)
	for _, b := range all {
		if isRemoteOrphan(b) {
			orphaned++
			if !opts.includeAll {
				continue
			}
		}
		blobs = append(blobs, b)
	}
	limit := len(blobs)
	if !opts.wide && limit > defaultBlobLimit {
		limit = defaultBlobLimit
	}
	suffixes := nameCollisionSuffixes(blobs, func(b *controlv1.BlobSummary) (string, string, bool) {
		return b.GetName(), peerKeyString(b.GetPublisher().GetPeerPub()), true
	})
	for _, b := range blobs[:limit] {
		hash := b.GetHash()
		if !opts.wide && len(hash) > shortHexLen {
			hash = hash[:shortHexLen]
		}
		local := ""
		if b.GetLocal() {
			local = "*"
		}
		name := b.GetName()
		switch {
		case isRemoteOrphan(b):
			name = "(orphaned)"
		case name == "":
			name = "-"
		default:
			if sfx := suffixes[namePeerKey{name, peerKeyString(b.GetPublisher().GetPeerPub())}]; sfx != "" {
				name += "-" + sfx
			}
		}
		sec.rows = append(sec.rows, []string{name, hash, fmt.Sprintf("%d", b.GetReplicas()), local})
	}
	switch {
	case len(blobs) > limit:
		sec.footer = fmt.Sprintf("%d more blobs (use --wide)", len(blobs)-limit)
	case orphaned > 0 && !opts.includeAll:
		sec.footer = fmt.Sprintf("%d orphaned blobs hidden (use --include-offline)", orphaned)
	}
	return sec
}

func isRemoteOrphan(b *controlv1.BlobSummary) bool { return b.GetOrphan() && !b.GetLocal() }

func collectSeedsSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "SEEDS",
		headers: []string{"NAME", "HASH", "STATUS", "REPLICAS", "LOCAL", "UPTIME"},
	}

	now := time.Now()
	clusterSize := uint32(len(st.GetNodes()) + 1) // +1 for self (not included in Nodes)
	for _, w := range st.GetWorkloads() {
		name := w.GetName()
		hash := w.GetHash()
		if name == hash || name == "" {
			name = "-"
		}
		if !opts.wide && len(hash) > shortHexLen {
			hash = hash[:shortHexLen]
		}
		st := formatWorkloadStatus(w.GetStatus())
		target := w.GetMinReplicas()
		if w.GetSpread() >= 1.0 {
			target = clusterSize
		}
		replicas := fmt.Sprintf("%d/%d", w.GetActiveReplicas(), target)
		local := ""
		if w.GetLocal() {
			local = "*"
		}
		uptime := "-"
		if w.GetStartedAtUnix() > 0 {
			uptime = humanDuration(now.Sub(time.Unix(w.GetStartedAtUnix(), 0)))
		}
		sec.rows = append(sec.rows, []string{name, hash, st, replicas, local, uptime})
	}
	return sec
}

// Terminal 256-colour code for a muted grey — used for secondary text
// (context label, table headers).
const mutedFG = lipgloss.Color("245")

func newStatusTable(headers ...string) *table.Table {
	headerStyle := lipgloss.NewStyle().Foreground(mutedFG).PaddingRight(tablePadding)
	dataStyle := lipgloss.NewStyle().PaddingRight(tablePadding)
	return table.New().
		Border(lipgloss.HiddenBorder()).
		BorderTop(false).BorderBottom(false).BorderLeft(false).BorderRight(false).
		BorderHeader(false).BorderColumn(false).
		Headers(headers...).
		StyleFunc(func(row, _ int) lipgloss.Style {
			if row == table.HeaderRow {
				return headerStyle
			}
			return dataStyle
		})
}

func renderStatusSections(w io.Writer, sections []statusSection) {
	sectionStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("4"))

	for i, sec := range sections {
		if i > 0 {
			fmt.Fprintln(w)
		}
		fmt.Fprintln(w, sectionStyle.Render(sec.title))

		hasRows := len(sec.rows) > 0
		if hasRows {
			t := newStatusTable(sec.headers...)
			for _, dataRow := range sec.rows {
				t.Row(dataRow...)
			}
			fmt.Fprintln(w, t)
		}
		if sec.footer != "" {
			if hasRows {
				fmt.Fprintln(w)
			}
			fmt.Fprintln(w, sec.footer)
		}
	}
	fmt.Fprintln(w)
}

func renderStatusHeader(w io.Writer, m *controlv1.GetMetricsResponse, contextLabel string) {
	var label, color string
	switch m.GetHealth() {
	case controlv1.HealthStatus_HEALTH_STATUS_HEALTHY:
		label, color = "HEALTHY", "2"
	case controlv1.HealthStatus_HEALTH_STATUS_DEGRADED:
		label, color = "DEGRADED", "3"
	case controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY:
		label, color = "UNHEALTHY", "1"
	case controlv1.HealthStatus_HEALTH_STATUS_UNSPECIFIED:
		// no health line
	}

	wrote := false
	if label != "" {
		fmt.Fprintln(w, lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(color)).Render(label))
		wrote = true
	}
	if contextLabel != "" {
		fmt.Fprintln(w, lipgloss.NewStyle().Foreground(mutedFG).Render("context: "+contextLabel))
		wrote = true
	}
	if wrote {
		fmt.Fprintln(w)
	}
}

// e.g. "prod (root@prod.example)", "dev (/tmp/pln-dev)", or bare "default".
func statusContextLabel(defaultDir string) string {
	name := resolveContextName()
	if name == "" {
		return ""
	}
	dir, host, err := resolveContextBindings(name, defaultDir)
	if err != nil {
		return name
	}
	switch {
	case host != "":
		return name + " (" + host + ")"
	case name != defaultContextName:
		return name + " (" + dir + ")"
	default:
		return name
	}
}

func renderMetricsDetails(w io.Writer, m *controlv1.GetMetricsResponse) {
	fmt.Fprintln(w, lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("4")).Render("DIAGNOSTICS"))
	fmt.Fprintf(w, "vivaldi error:    %.3f\n", m.GetVivaldiError())
	fmt.Fprintf(w, "vivaldi samples:  %d\n", m.GetVivaldiSamples())
	fmt.Fprintf(w, "eager syncs:      %d ok, %d failed\n", m.GetEagerSyncs(), m.GetEagerSyncFailures())
	if m.GetPunchAttempts() > 0 {
		fmt.Fprintf(w, "punch success:    %d/%d\n", m.GetPunchAttempts()-m.GetPunchFailures(), m.GetPunchAttempts())
	}
	if m.GetCertRenewals() > 0 || m.GetCertRenewalsFailed() > 0 {
		fmt.Fprintf(w, "cert renewals:    %d ok, %d failed\n", m.GetCertRenewals(), m.GetCertRenewalsFailed())
	}
	fmt.Fprintln(w)
}

func certExpiryFooter(st *controlv1.GetStatusResponse) string {
	const certExpirySkew = time.Minute
	var latest time.Time
	var health controlv1.CertHealth
	for _, c := range st.GetCertificates() {
		if t := time.Unix(c.GetNotAfterUnix(), 0); t.After(latest) {
			latest, health = t, c.GetHealth()
		}
	}
	if latest.IsZero() {
		return ""
	}

	remaining := time.Until(latest.Add(certExpirySkew))
	var latestDeadline int64
	for _, c := range st.GetCertificates() {
		if dl := c.GetAccessDeadlineUnix(); dl > latestDeadline {
			latestDeadline = dl
		}
	}
	hasDeadline := latestDeadline > 0

	if remaining <= 0 || health == controlv1.CertHealth_CERT_HEALTH_EXPIRED {
		if hasDeadline {
			return lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render("temporary access expired — rejoin the cluster or contact a cluster admin")
		}
		return lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render("membership expired — entering degraded mode; will auto-recover when an admin peer is reachable")
	}

	msg := "membership expires in " + humanDuration(remaining)
	if hasDeadline {
		msg = "temporary access expires in " + humanDuration(time.Until(time.Unix(latestDeadline, 0)))
	}

	switch health { //nolint:exhaustive
	case controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Render(msg + " — auto-renewal failed — rejoin the cluster or contact a cluster admin")
	case controlv1.CertHealth_CERT_HEALTH_RENEWING:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Render(msg + " — auto-renewal in progress")
	default:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("2")).Render(msg)
	}
}

func renderCertAttributes(w io.Writer, st *controlv1.GetStatusResponse) {
	for _, c := range st.GetCertificates() {
		a := c.GetAttributes()
		if a == nil || len(a.GetFields()) == 0 {
			continue
		}
		keys := make([]string, 0, len(a.GetFields()))
		for k := range a.GetFields() {
			keys = append(keys, k)
		}
		slices.Sort(keys)

		fmt.Fprintln(w, lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("4")).Render("ATTRIBUTES"))
		for _, k := range keys {
			fmt.Fprintf(w, "%-16s%s\n", k, formatStructValue(a.GetFields()[k]))
		}
		fmt.Fprintln(w)
		return
	}
}

func formatStructValue(v *structpb.Value) string {
	switch v.GetKind().(type) {
	case *structpb.Value_StringValue:
		return v.GetStringValue()
	case *structpb.Value_NumberValue:
		return strconv.FormatFloat(v.GetNumberValue(), 'f', -1, 64)
	case *structpb.Value_BoolValue:
		return strconv.FormatBool(v.GetBoolValue())
	default:
		return protojson.Format(v)
	}
}

// --- Formatters ---

func formatCount(v uint32) string {
	if v == 0 {
		return "-"
	}
	return fmt.Sprintf("%d", v)
}

func formatPercent(v uint32) string {
	if v == 0 {
		return "-"
	}
	return fmt.Sprintf("%d%%", v)
}

func formatRate(bytesPerSec uint64) string {
	if bytesPerSec == 0 {
		return "-"
	}
	const kb, mb, gb = 1024, 1024 * 1024, 1024 * 1024 * 1024
	switch {
	case bytesPerSec >= gb:
		return fmt.Sprintf("%.1f GB/s", float64(bytesPerSec)/float64(gb))
	case bytesPerSec >= mb:
		return fmt.Sprintf("%.1f MB/s", float64(bytesPerSec)/float64(mb))
	case bytesPerSec >= kb:
		return fmt.Sprintf("%.1f KB/s", float64(bytesPerSec)/float64(kb))
	default:
		return fmt.Sprintf("%d B/s", bytesPerSec)
	}
}

func formatLatency(ms float64) string {
	if ms == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1fms", ms)
}

func formatWorkloadStatus(s controlv1.WorkloadStatus) string {
	if s == controlv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		return "running"
	}
	return "unknown"
}

func humanDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "<1m"
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < hoursPerDay*time.Hour:
		h, m := int(d.Hours()), int(d.Minutes())%minutesPerHour
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh %dm", h, m)
	case d < 7*hoursPerDay*time.Hour:
		days, hours := int(d.Hours())/hoursPerDay, int(d.Hours())%hoursPerDay
		if hours == 0 {
			return fmt.Sprintf("%dd", days)
		}
		return fmt.Sprintf("%dd %dh", days, hours)
	case d < daysPerYear*hoursPerDay*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours())/hoursPerDay)
	default:
		days := int(d.Hours()) / hoursPerDay
		y, rem := days/daysPerYear, days%daysPerYear
		if rem == 0 {
			return fmt.Sprintf("%dy", y)
		}
		return fmt.Sprintf("%dy %dd", y, rem)
	}
}

func formatStatus(s controlv1.NodeStatus) string {
	switch s {
	case controlv1.NodeStatus_NODE_STATUS_ONLINE:
		return "direct"
	case controlv1.NodeStatus_NODE_STATUS_INDIRECT:
		return "indirect"
	default:
		return "offline"
	}
}

func isReachableStatus(s controlv1.NodeStatus) bool {
	return s == controlv1.NodeStatus_NODE_STATUS_ONLINE || s == controlv1.NodeStatus_NODE_STATUS_INDIRECT
}

func socketPermissionDenied(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, socketName))
	return errors.Is(err, os.ErrPermission)
}

func formatPeerID(peerID []byte, wide bool) string {
	full := peerKeyString(peerID)
	if full == "" {
		return "-"
	}
	if wide || len(full) <= shortHexLen {
		return full
	}
	return full[:shortHexLen]
}

func peerKeyString(peerID []byte) string {
	if len(peerID) == 0 {
		return ""
	}
	return types.PeerKeyFromBytes(peerID).String()
}

func peerIDHasPrefix(peerID []byte, prefix string) bool {
	return strings.HasPrefix(peerKeyString(peerID), strings.ToLower(prefix))
}

func isPortArg(s string) bool {
	p, err := strconv.Atoi(s)
	return err == nil && p >= minPort && p <= maxPort
}

func resolveServiceByPrefix(services []config.Service, prefix string) (string, error) {
	var matches []string
	for _, s := range services {
		if strings.HasPrefix(s.Name, prefix) {
			matches = append(matches, s.Name)
		}
	}
	switch len(matches) {
	case 0:
		return "", notFoundErr("no service matching %q", prefix)
	case 1:
		return matches[0], nil
	default:
		return "", ambiguousErr("prefix %q matches multiple services: %s", prefix, strings.Join(matches, ", "))
	}
}

// --- Resolution & Collision Logic ---

type suffixCandidate struct {
	name    string
	peerKey string
	include bool
}

// namePeerKey identifies a named entity published by a specific peer.
type namePeerKey struct {
	name    string
	peerKey string
}

func reachableProviderSet(st *controlv1.GetStatusResponse) map[string]bool {
	m := map[string]bool{}
	if st.GetSelf() != nil && st.GetSelf().GetNode() != nil {
		m[peerKeyString(st.GetSelf().GetNode().GetPeerPub())] = true
	}
	for _, n := range st.Nodes {
		if isReachableStatus(n.GetStatus()) {
			m[peerKeyString(n.GetNode().GetPeerPub())] = true
		}
	}
	return m
}

func resolveService(st *controlv1.GetStatusResponse, serviceArg, providerArg string) (*controlv1.ServiceSummary, error) {
	reachableProviders := reachableProviderSet(st)
	portFilter := uint32(0)
	if p, err := strconv.Atoi(serviceArg); err == nil && p >= minPort && p <= maxPort {
		portFilter = uint32(p)
	}

	matches := make([]*controlv1.ServiceSummary, 0, len(st.Services))
	for _, svc := range st.Services {
		if portFilter > 0 {
			if svc.GetPort() != portFilter && svc.GetName() != serviceArg {
				continue
			}
		} else if svc.GetName() != serviceArg {
			continue
		}

		providerID := peerKeyString(svc.GetProvider().GetPeerPub())
		if !reachableProviders[providerID] {
			continue
		}
		if providerArg != "" && !peerIDHasPrefix(svc.GetProvider().GetPeerPub(), providerArg) {
			continue
		}
		matches = append(matches, svc)
	}

	if len(matches) == 1 {
		return matches[0], nil
	}

	if len(matches) > 1 {
		return nil, serviceCollisionError(serviceArg, matches)
	}

	if providerArg == "" && portFilter == 0 {
		if svc, err := resolveBySuffix(serviceArg, st.Services, func(svc *controlv1.ServiceSummary) suffixCandidate {
			pk := peerKeyString(svc.GetProvider().GetPeerPub())
			return suffixCandidate{name: svc.GetName(), peerKey: pk, include: reachableProviders[pk]}
		}, serviceCollisionError); !errors.Is(err, errNoSuffixMatch) {
			return svc, err
		}
	}

	if providerArg != "" {
		return nil, notFoundErr("no reachable provider for %q on %q — run \"pln status\" to see available services", serviceArg, providerArg)
	}
	return nil, notFoundErr("no reachable service matching %q — run \"pln status\" to see available services", serviceArg)
}

func resolveConnection(st *controlv1.GetStatusResponse, arg, providerArg string) (*controlv1.ConnectionSummary, error) {
	var matches []*controlv1.ConnectionSummary

	if providerArg == "" && isPortArg(arg) {
		localPort := uint32(func() int { p, _ := strconv.Atoi(arg); return p }())
		for _, c := range st.GetConnections() {
			if c.GetLocalPort() == localPort {
				matches = append(matches, c)
			}
		}
	}

	if len(matches) == 0 {
		for _, c := range st.GetConnections() {
			if c.GetServiceName() == arg {
				if providerArg != "" && !peerIDHasPrefix(c.GetPeer().GetPeerPub(), providerArg) {
					continue
				}
				matches = append(matches, c)
			}
		}
	}

	if len(matches) == 1 {
		return matches[0], nil
	}

	if len(matches) > 1 {
		return nil, connectionCollisionError(arg, matches)
	}

	if providerArg == "" && !isPortArg(arg) {
		if c, err := resolveBySuffix(arg, st.GetConnections(), func(c *controlv1.ConnectionSummary) suffixCandidate {
			return suffixCandidate{name: c.GetServiceName(), peerKey: peerKeyString(c.GetPeer().GetPeerPub()), include: true}
		}, connectionCollisionError); !errors.Is(err, errNoSuffixMatch) {
			return c, err
		}
	}

	return nil, notFoundErr("no active connection matching %q — run \"pln status\" to see active connections", arg)
}

func resolveBySuffix[T any](arg string, items []T, toCandidate func(T) suffixCandidate, collisionErr func(string, []T) error) (T, error) {
	names := map[string]bool{}
	candidates := make([]suffixCandidate, 0, len(items))
	for _, item := range items {
		c := toCandidate(item)
		if c.name != "" {
			names[c.name] = true
		}
		candidates = append(candidates, c)
	}

	indices, name, err := matchSuffixCandidates(arg, names, candidates)
	if err != nil {
		var zero T
		return zero, err
	}

	matches := make([]T, len(indices))
	for i, idx := range indices {
		matches[i] = items[idx]
	}
	if len(matches) > 1 {
		var zero T
		return zero, collisionErr(name, matches)
	}
	return matches[0], nil
}

func matchSuffixCandidates(arg string, knownNames map[string]bool, items []suffixCandidate) ([]int, string, error) {
	name, prefix, ok := parseSuffixedArg(arg, knownNames)
	if !ok {
		return nil, "", errNoSuffixMatch
	}

	var indices []int
	for i, item := range items {
		if item.name == name && item.include && strings.HasPrefix(item.peerKey, prefix) {
			indices = append(indices, i)
		}
	}
	if len(indices) == 0 {
		return nil, name, errNoSuffixMatch
	}
	return indices, name, nil
}

func parseSuffixedArg(arg string, knownNames map[string]bool) (name, prefix string, ok bool) {
	for n := range knownNames {
		if !strings.HasPrefix(arg, n+"-") {
			continue
		}
		p := arg[len(n)+1:]
		if p != "" && len(n) > len(name) {
			name, prefix, ok = n, p, true
		}
	}
	return name, prefix, ok
}

func serviceCollisionError(name string, matches []*controlv1.ServiceSummary) error {
	ids := make([]string, len(matches))
	for i, svc := range matches {
		ids[i] = peerKeyString(svc.GetProvider().GetPeerPub())
	}
	prefixes := minUniquePrefixes(ids)

	var b strings.Builder
	fmt.Fprintf(&b, "multiple services match %q — pick one:\n", name)
	for i, svc := range matches {
		fmt.Fprintf(&b, "  pln connect %s-%s    (%s:%d)\n", name, prefixes[i], formatPeerID(svc.GetProvider().GetPeerPub(), false), svc.GetPort())
	}
	return errors.New(strings.TrimSpace(b.String()))
}

func connectionCollisionError(name string, matches []*controlv1.ConnectionSummary) error {
	ids := make([]string, len(matches))
	for i, c := range matches {
		ids[i] = peerKeyString(c.GetPeer().GetPeerPub())
	}
	prefixes := minUniquePrefixes(ids)

	var b strings.Builder
	fmt.Fprintf(&b, "multiple connections match %q — pick one:\n", name)
	for i, c := range matches {
		connName := c.GetServiceName()
		if connName == "" {
			connName = strconv.FormatUint(uint64(c.GetRemotePort()), 10)
		}
		fmt.Fprintf(&b, "  pln disconnect %s-%s    (localhost:%d -> %s:%d)\n", connName, prefixes[i], c.GetLocalPort(), formatPeerID(c.GetPeer().GetPeerPub(), false), c.GetRemotePort())
	}
	return errors.New(strings.TrimSpace(b.String()))
}

// nodeNameLabels builds display labels for named nodes, keyed by hex peer ID.
// In condensed mode the shortest unique prefix across all peers is appended in
// brackets so users can paste it into commands like `pln grant`; wide mode
// shows the full ID in parentheses instead.
func nodeNameLabels(self *controlv1.NodeSummary, peers []*controlv1.NodeSummary, wide bool) map[string]string {
	// Collect all peer IDs (named and unnamed) so the prefix is unique
	// across the entire cluster, not just named nodes.
	var allIDs []string
	nameByPeer := map[string]string{}

	if self != nil {
		pk := peerKeyString(self.GetNode().GetPeerPub())
		allIDs = append(allIDs, pk)
		if self.GetName() != "" {
			nameByPeer[pk] = self.GetName()
		}
	}
	for _, n := range peers {
		pk := peerKeyString(n.GetNode().GetPeerPub())
		allIDs = append(allIDs, pk)
		if n.GetName() != "" {
			nameByPeer[pk] = n.GetName()
		}
	}

	// Build a peer-ID → shortest-unique-prefix map for condensed mode.
	prefixByPeer := map[string]string{}
	if !wide && len(allIDs) > 0 {
		pfx := minUniquePrefixes(allIDs)
		for i, id := range allIDs {
			prefixByPeer[id] = pfx[i]
		}
	}

	labels := make(map[string]string, len(nameByPeer))
	for pk, name := range nameByPeer {
		if wide {
			labels[pk] = name + " (" + pk + ")"
		} else {
			labels[pk] = name + " [" + prefixByPeer[pk] + "]"
		}
	}
	return labels
}

// nameCollisionSuffixes returns the minimum-unique peer-key prefix for
// each item sharing a name with another peer, so callers can render
// disambiguation hints like "config-abc" vs "config-def". Items where
// extract returns include=false or an empty name are skipped.
func nameCollisionSuffixes[T any](items []T, extract func(T) (string, string, bool)) map[namePeerKey]string {
	groups := map[string][]string{}
	for _, item := range items {
		name, pk, include := extract(item)
		if !include || name == "" {
			continue
		}
		groups[name] = append(groups[name], pk)
	}

	result := map[namePeerKey]string{}
	for name, pks := range groups {
		if len(pks) < minDisambigPeers {
			continue
		}
		prefixes := minUniquePrefixes(pks)
		for i, pk := range pks {
			result[namePeerKey{name, pk}] = prefixes[i]
		}
	}
	return result
}

func minUniquePrefixes(ids []string) []string {
	n := len(ids)
	out := make([]string, n)
	for i, id := range ids {
		prefixLen := 1
		for j, other := range ids {
			if i == j {
				continue
			}
			common := 0
			for common < len(id) && common < len(other) && id[common] == other[common] {
				common++
			}
			if common+1 > prefixLen {
				prefixLen = common + 1
			}
		}
		if prefixLen > len(id) {
			prefixLen = len(id)
		}
		out[i] = id[:prefixLen]
	}
	return out
}
