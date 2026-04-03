package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/types"
)

var errNoSuffixMatch = errors.New("no suffix match")

const (
	hoursPerDay      = 24
	daysPerYear      = 365
	minutesPerHour   = 60
	tablePadding     = 2
	minDisambigPeers = 2
)

func newNetworkCmds() []*cobra.Command {
	statusCmd := &cobra.Command{
		Use:   "status [nodes|services|seeds]",
		Short: "Show network status",
		Args:  cobra.RangeArgs(0, 1),
		RunE:  withEnv(false, runStatus),
	}
	statusCmd.Flags().Bool("wide", false, "Show full peer IDs and extra details")
	statusCmd.Flags().Bool("all", false, "Include offline nodes and services")

	serveCmd := &cobra.Command{
		Use:   "serve <port> [name]",
		Short: "Expose a local port to the mesh",
		Args:  cobra.RangeArgs(1, 2), //nolint:mnd
		RunE:  withEnv(false, runServe),
	}

	unserveCmd := &cobra.Command{
		Use:   "unserve <port|name>",
		Short: "Stop exposing a local service",
		Args:  cobra.ExactArgs(1),
		RunE:  withEnv(false, runUnserve),
	}

	connectCmd := &cobra.Command{
		Use:   "connect <service> [provider] [local-port]",
		Short: "Tunnel a local port to a service",
		Args:  cobra.RangeArgs(1, 3), //nolint:mnd
		RunE:  withEnv(false, runConnect),
	}

	disconnectCmd := &cobra.Command{
		Use:   "disconnect <service|local-port> [provider]",
		Short: "Close a tunnel to a service",
		Args:  cobra.RangeArgs(1, 2), //nolint:mnd
		RunE:  withEnv(false, runDisconnect),
	}

	denyCmd := &cobra.Command{
		Use:   "deny <peer-id>",
		Short: "Deny a peer's membership",
		Args:  cobra.ExactArgs(1),
		RunE:  withEnv(false, runDeny),
	}

	return []*cobra.Command{statusCmd, serveCmd, unserveCmd, connectCmd, disconnectCmd, denyCmd}
}

// --- Command Runners ---

func runStatus(cmd *cobra.Command, args []string, env *cliEnv) error {
	mode := "all"
	if len(args) == 1 {
		mode = args[0]
	}
	wide, _ := cmd.Flags().GetBool("wide")
	includeAll, _ := cmd.Flags().GetBool("all")
	opts := statusViewOpts{wide: wide, includeAll: includeAll}

	resp, err := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeUnavailable {
			return err
		}
		if socketPermissionDenied(env.dir) {
			return errors.New("cannot reach daemon — are you in the pln group?\n  fix: sudo usermod -aG pln $(whoami) && newgrp pln")
		}
		return errors.New("daemon is not running")
	}

	st := resp.Msg
	metricsResp, metricsErr := env.client.GetMetrics(cmd.Context(), connect.NewRequest(&controlv1.GetMetricsRequest{}))
	if metricsErr == nil {
		renderHealthLine(cmd.OutOrStdout(), metricsResp.Msg)
	}

	var sections []statusSection
	switch mode {
	case "all":
		if s := collectPeersSection(st, opts); len(s.rows) > 0 {
			sections = append(sections, s)
		}
		if s := collectServicesSection(st, opts); len(s.rows) > 0 {
			sections = append(sections, s)
		}
		if s := collectSeedsSection(st, opts); len(s.rows) > 0 {
			sections = append(sections, s)
		}
	case "nodes", "node":
		if s := collectPeersSection(st, opts); len(s.rows) > 0 {
			sections = append(sections, s)
		}
	case "services", "service", "serve":
		if s := collectServicesSection(st, opts); len(s.rows) > 0 {
			sections = append(sections, s)
		}
	case "seeds", "seed":
		if s := collectSeedsSection(st, opts); len(s.rows) > 0 {
			sections = append(sections, s)
		}
	default:
		return fmt.Errorf("unknown status selector %q (use: nodes|services|seeds)", mode)
	}

	renderStatusSections(cmd.OutOrStdout(), sections)

	if wide && metricsErr == nil {
		renderMetricsDetails(cmd.OutOrStdout(), metricsResp.Msg)
	}

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

	env.cfg.AddService(name, uint32(port))

	sockPath := filepath.Join(env.dir, socketName)
	// Catching only the single bool value
	if nodeSocketActive(sockPath) {
		req := &controlv1.RegisterServiceRequest{Port: uint32(port)}
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
	// Catching only the single bool value
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
	}))
	if err != nil {
		return err
	}

	actualLocalPort := connectResp.Msg.GetLocalPort()
	provider := formatPeerID(svc.GetProvider().GetPeerPub(), false)
	peerHex := peerKeyString(svc.GetProvider().GetPeerPub())

	env.cfg.AddConnection(svc.GetName(), peerHex, svc.GetPort(), actualLocalPort)
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
		return fmt.Errorf("no peer matching %q", prefix)
	}
	if len(matches) > 1 {
		return fmt.Errorf("ambiguous peer prefix %q matches %d peers", prefix, len(matches))
	}

	peerID := matches[0]
	if _, err := env.client.DenyPeer(cmd.Context(), connect.NewRequest(&controlv1.DenyPeerRequest{PeerPub: peerID})); err != nil {
		return err
	}

	env.cfg.ForgetBootstrapPeer(peerID)
	if saveErr := config.Save(env.dir, env.cfg); saveErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to persist denial to config: %v\n", saveErr)
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

func collectPeersSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "PEERS",
		headers: []string{"NODE", "STATUS", "ADDR", "CPUs", "CPU", "MEM", "TUNNELS", "LATENCY", "TRAFFIC IN", "TRAFFIC OUT"},
	}

	if self := st.GetSelf(); self != nil && self.GetNode() != nil {
		label := formatPeerID(self.GetNode().GetPeerPub(), opts.wide) + " (self)"
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
			formatBytes(self.GetTrafficBytesIn()), formatBytes(self.GetTrafficBytesOut()),
		})
	}

	filtered := 0
	for _, n := range st.Nodes {
		if !opts.includeAll && !isReachableStatus(n.GetStatus()) {
			filtered++
			continue
		}
		label := formatPeerID(n.GetNode().GetPeerPub(), opts.wide)
		status := formatStatus(n.GetStatus())
		if n.GetPubliclyAccessible() {
			status += " (public)"
		}
		addr := n.GetAddr()
		if addr == "" {
			addr = "-"
		}

		cpus, cpu, mem, tunnels, latency, trafficIn, trafficOut := formatCount(n.GetNumCpu()), formatPercent(n.GetCpuPercent()), formatPercent(n.GetMemPercent()), formatCount(n.GetTunnelCount()), formatLatency(n.GetLatencyMs()), formatBytes(n.GetTrafficBytesIn()), formatBytes(n.GetTrafficBytesOut())

		if !isReachableStatus(n.GetStatus()) {
			cpus, cpu, mem, tunnels, latency, trafficIn, trafficOut = "-", "-", "-", "-", "-", "-", "-"
		}

		sec.rows = append(sec.rows, []string{label, status, addr, cpus, cpu, mem, tunnels, latency, trafficIn, trafficOut})
	}

	if filtered > 0 {
		sec.footer = fmt.Sprintf("offline peers: %d (use --all)", filtered)
	}
	return sec
}

func collectServicesSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "SERVICES",
		headers: []string{"SERVICE", "PROVIDER", "PORT", "LOCAL"},
	}

	reachableProviders := reachableProviderSet(st)
	suffixes := serviceNameSuffixes(st.Services, func(pk string) bool { return opts.includeAll || reachableProviders[pk] })

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
		provider := formatPeerID(s.GetProvider().GetPeerPub(), opts.wide)
		local := "-"
		if lp, ok := connLocal[connKey{s.GetName(), providerID}]; ok {
			local = fmt.Sprintf("%d", lp)
		}
		displayName := s.GetName()
		if sfx := suffixes[serviceProviderKey{s.GetName(), providerID}]; sfx != "" {
			displayName += "-" + sfx
		}
		sec.rows = append(sec.rows, []string{displayName, provider, fmt.Sprintf("%d", s.GetPort()), local})
	}

	var footerParts []string
	if hasCollisions {
		footerParts = append(footerParts, "service suffixes match the start of the provider ID")
	}
	if filtered > 0 {
		footerParts = append(footerParts, fmt.Sprintf("offline services: %d (use --all)", filtered))
	}
	sec.footer = strings.Join(footerParts, "\n")
	return sec
}

func collectSeedsSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "SEEDS",
		headers: []string{"HASH", "STATUS", "REPLICAS", "LOCAL", "UPTIME"},
	}

	now := time.Now()
	for _, w := range st.GetWorkloads() {
		hash := w.GetHash()
		if !opts.wide && len(hash) > 16 {
			hash = hash[:16]
		}
		st := formatWorkloadStatus(w.GetStatus())
		replicas := fmt.Sprintf("%d/%d", w.GetActiveReplicas(), w.GetDesiredReplicas())
		local := ""
		if w.GetLocal() {
			local = "*"
		}
		uptime := "-"
		if w.GetStartedAtUnix() > 0 {
			uptime = humanDuration(now.Sub(time.Unix(w.GetStartedAtUnix(), 0)))
		}
		sec.rows = append(sec.rows, []string{hash, st, replicas, local, uptime})
	}
	return sec
}

func renderStatusSections(w io.Writer, sections []statusSection) {
	sectionStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("4"))
	headerStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("245")).PaddingRight(tablePadding)
	dataStyle := lipgloss.NewStyle().PaddingRight(tablePadding)

	for i, sec := range sections {
		if i > 0 {
			fmt.Fprintln(w)
		}
		fmt.Fprintln(w, sectionStyle.Render(sec.title))

		t := table.New().
			Border(lipgloss.HiddenBorder()).
			BorderTop(false).BorderBottom(false).BorderLeft(false).BorderRight(false).
			BorderHeader(false).BorderColumn(false).
			Headers(sec.headers...)

		for _, dataRow := range sec.rows {
			t.Row(dataRow...)
		}

		t.StyleFunc(func(row, _ int) lipgloss.Style {
			if row == table.HeaderRow {
				return headerStyle
			}
			return dataStyle
		})

		fmt.Fprintln(w, t)

		if sec.footer != "" {
			fmt.Fprintln(w)
			fmt.Fprintln(w, sec.footer)
		}
	}
	fmt.Fprintln(w)
}

func renderHealthLine(w io.Writer, m *controlv1.GetMetricsResponse) {
	var label, color string
	switch m.GetHealth() {
	case controlv1.HealthStatus_HEALTH_STATUS_HEALTHY:
		label, color = "HEALTHY", "2"
	case controlv1.HealthStatus_HEALTH_STATUS_DEGRADED:
		label, color = "DEGRADED", "3"
	case controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY:
		label, color = "UNHEALTHY", "1"
	case controlv1.HealthStatus_HEALTH_STATUS_UNSPECIFIED:
		return
	}
	fmt.Fprintln(w, lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(color)).Render(label))
	fmt.Fprintln(w)
}

func renderMetricsDetails(w io.Writer, m *controlv1.GetMetricsResponse) {
	fmt.Fprintln(w, lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("4")).Render("DIAGNOSTICS"))
	fmt.Fprintf(w, "  vivaldi error:    %.3f\n", m.GetVivaldiError())
	fmt.Fprintf(w, "  vivaldi samples:  %d\n", m.GetVivaldiSamples())
	fmt.Fprintf(w, "  eager syncs:      %d ok, %d failed\n", m.GetEagerSyncs(), m.GetEagerSyncFailures())
	if m.GetPunchAttempts() > 0 {
		fmt.Fprintf(w, "  punch success:    %d/%d\n", m.GetPunchAttempts()-m.GetPunchFailures(), m.GetPunchAttempts())
	}
	if m.GetCertRenewals() > 0 || m.GetCertRenewalsFailed() > 0 {
		fmt.Fprintf(w, "  cert renewals:    %d ok, %d failed\n", m.GetCertRenewals(), m.GetCertRenewalsFailed())
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

func formatBytes(b uint64) string {
	if b == 0 {
		return "-"
	}
	const kb, mb, gb = 1024, 1024 * 1024, 1024 * 1024 * 1024
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func formatLatency(ms float64) string {
	if ms == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1fms", ms)
}

func formatWorkloadStatus(s controlv1.WorkloadStatus) string {
	switch s {
	case controlv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		return "running"
	case controlv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED:
		return "stopped"
	case controlv1.WorkloadStatus_WORKLOAD_STATUS_ERRORED:
		return "errored"
	default:
		return "unknown"
	}
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
	if wide || len(full) <= 8 {
		return full
	}
	return full[:8]
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
		return "", fmt.Errorf("no service matching %q", prefix)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("prefix %q matches multiple services: %s", prefix, strings.Join(matches, ", "))
	}
}

// --- Resolution & Collision Logic ---

type suffixCandidate struct {
	name    string
	peerKey string
	include bool
}

type serviceProviderKey struct {
	name     string
	provider string
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
		return nil, fmt.Errorf("no reachable provider for %q on %q — run \"pln status\" to see available services", serviceArg, providerArg)
	}
	return nil, fmt.Errorf("no reachable service matching %q — run \"pln status\" to see available services", serviceArg)
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

	return nil, fmt.Errorf("no active connection matching %q — run \"pln status\" to see active connections", arg)
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

func serviceNameSuffixes(services []*controlv1.ServiceSummary, include func(string) bool) map[serviceProviderKey]string {
	groups := map[string][]string{}
	for _, svc := range services {
		if pk := peerKeyString(svc.GetProvider().GetPeerPub()); include(pk) {
			groups[svc.GetName()] = append(groups[svc.GetName()], pk)
		}
	}

	result := map[serviceProviderKey]string{}
	for name, pks := range groups {
		if len(pks) < minDisambigPeers {
			continue
		}
		prefixes := minUniquePrefixes(pks)
		for i, pk := range pks {
			result[serviceProviderKey{name, pk}] = prefixes[i]
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
