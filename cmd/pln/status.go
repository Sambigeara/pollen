package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

func newStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status [nodes|services]",
		Short: "Show status",
		Args:  cobra.RangeArgs(0, 1),
		Run:   runStatus,
	}
	cmd.Flags().Bool("wide", false, "Show full peer IDs and extra details")
	cmd.Flags().Bool("all", false, "Include offline nodes and services")
	return cmd
}

func runStatus(cmd *cobra.Command, args []string) {
	mode := "all"
	if len(args) == 1 {
		mode = args[0]
	}
	wide, _ := cmd.Flags().GetBool("wide")
	includeAll, _ := cmd.Flags().GetBool("all")
	opts := statusViewOpts{wide: wide, includeAll: includeAll}

	client := newControlClient(cmd)
	resp, err := client.GetStatus(context.Background(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeUnavailable {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
			os.Exit(1)
		}
		if socketPermissionDenied(cmd) {
			fmt.Fprintf(cmd.ErrOrStderr(),
				"cannot reach daemon — are you in the pln group?\n"+
					"  fix: sudo usermod -aG pln $(whoami) && newgrp pln\n")
			os.Exit(1)
		}
		fmt.Fprintln(cmd.ErrOrStderr(), "daemon is not running")
		os.Exit(1)
	}

	st := resp.Msg

	metricsResp, metricsErr := client.GetMetrics(context.Background(), connect.NewRequest(&controlv1.GetMetricsRequest{}))
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
	case "nodes", "node":
		if s := collectPeersSection(st, opts); len(s.rows) > 0 {
			sections = append(sections, s)
		}
	case "services", "service", "serve":
		if s := collectServicesSection(st, opts); len(s.rows) > 0 {
			sections = append(sections, s)
		}
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unknown status selector %q (use: nodes|services)\n", mode)
		return
	}
	renderStatusSections(cmd.OutOrStdout(), sections)

	if wide && metricsErr == nil {
		renderMetricsDetails(cmd.OutOrStdout(), metricsResp.Msg)
	}

	if footer := certExpiryFooter(st); footer != "" {
		fmt.Fprintln(cmd.OutOrStdout(), footer)
	}
}

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
		headers: []string{"NODE", "STATUS", "ADDR", "CPU", "MEM", "TUNNELS", "LATENCY"},
	}

	if self := st.GetSelf(); self != nil && self.GetNode() != nil {
		label := formatPeerID(self.GetNode().GetPeerId(), opts.wide) + " (self)"
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
			formatPercent(self.GetCpuPercent()),
			formatPercent(self.GetMemPercent()),
			formatTunnelCount(self.GetTunnelCount()),
			"-",
		})
	}

	filtered := 0
	for _, n := range st.Nodes {
		if !opts.includeAll && !isReachableStatus(n.GetStatus()) {
			filtered++
			continue
		}
		label := formatPeerID(n.GetNode().GetPeerId(), opts.wide)
		status := formatStatus(n.GetStatus())
		if n.GetPubliclyAccessible() {
			status += " (public)"
		}
		addr := n.GetAddr()
		if addr == "" {
			addr = "-"
		}

		cpu := formatPercent(n.GetCpuPercent())
		mem := formatPercent(n.GetMemPercent())
		tunnels := formatTunnelCount(n.GetTunnelCount())
		latency := formatLatency(n.GetLatencyMs())

		if !isReachableStatus(n.GetStatus()) {
			cpu = "-"
			mem = "-"
			tunnels = "-"
			latency = "-"
		}

		sec.rows = append(sec.rows, []string{label, status, addr, cpu, mem, tunnels, latency})
	}

	if filtered > 0 {
		sec.footer = fmt.Sprintf("offline peers: %d (use --all)", filtered)
	}
	return sec
}

func formatPercent(v uint32) string {
	if v == 0 {
		return "-"
	}
	return fmt.Sprintf("%d%%", v)
}

func formatTunnelCount(v uint32) string {
	if v == 0 {
		return "-"
	}
	return fmt.Sprintf("%d", v)
}

func formatLatency(ms float64) string {
	if ms == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1fms", ms)
}

func collectServicesSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "SERVICES",
		headers: []string{"SERVICE", "PROVIDER", "PORT", "LOCAL"},
	}

	reachableProviders := reachableProviderSet(st)

	suffixes := serviceNameSuffixes(st.Services, func(pk string) bool {
		return opts.includeAll || reachableProviders[pk]
	})

	type connKey struct {
		service  string
		provider string
	}
	connLocal := map[connKey]uint32{}
	for _, c := range st.Connections {
		k := connKey{
			service:  c.GetServiceName(),
			provider: peerKeyString(c.GetPeer().GetPeerId()),
		}
		connLocal[k] = c.GetLocalPort()
	}

	filtered := 0
	hasCollisions := len(suffixes) > 0
	for _, s := range st.Services {
		providerID := peerKeyString(s.GetProvider().GetPeerId())
		if !opts.includeAll && !reachableProviders[providerID] {
			filtered++
			continue
		}
		provider := formatPeerID(s.GetProvider().GetPeerId(), opts.wide)
		local := "-"
		if lp, ok := connLocal[connKey{service: s.GetName(), provider: providerID}]; ok {
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

func certExpiryFooter(st *controlv1.GetStatusResponse) string {
	const certExpirySkew = time.Minute

	var latest time.Time
	var health controlv1.CertHealth
	var nonRenewable bool
	for _, c := range st.GetCertificates() {
		t := time.Unix(c.GetNotAfterUnix(), 0)
		if t.After(latest) {
			latest = t
			health = c.GetHealth()
			nonRenewable = c.GetNonRenewable()
		}
	}
	if latest.IsZero() {
		return ""
	}

	remaining := time.Until(latest.Add(certExpirySkew))

	if remaining <= 0 || health == controlv1.CertHealth_CERT_HEALTH_EXPIRED {
		msg := "membership expired — node has stopped; rejoin the cluster or contact a cluster admin"
		if nonRenewable {
			msg = "guest membership expired — node has stopped; request a new invite from a cluster admin"
		}
		return lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render(msg) //nolint:mnd
	}

	if nonRenewable {
		msg := "guest membership expires in " + humanDuration(remaining)
		if health == controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON {
			return lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Render(msg + " — request a new invite to continue") //nolint:mnd
		}
		return lipgloss.NewStyle().Foreground(lipgloss.Color("2")).Render(msg) //nolint:mnd
	}

	msg := "membership expires in " + humanDuration(remaining)
	if health == controlv1.CertHealth_CERT_HEALTH_RENEWING {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Render( //nolint:mnd
			msg + " — auto-renewal in progress")
	}
	if health == controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Render( //nolint:mnd
			msg + " — auto-renewal failed — rejoin the cluster or contact a cluster admin")
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color("2")).Render(msg) //nolint:mnd
}

func humanDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "<1m"
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		h := int(d.Hours())
		m := int(d.Minutes()) % 60 //nolint:mnd
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh %dm", h, m)
	case d < 7*24*time.Hour:
		days := int(d.Hours()) / 24  //nolint:mnd
		hours := int(d.Hours()) % 24 //nolint:mnd
		if hours == 0 {
			return fmt.Sprintf("%dd", days)
		}
		return fmt.Sprintf("%dd %dh", days, hours)
	case d < 365*24*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours())/24) //nolint:mnd
	default:
		days := int(d.Hours()) / 24 //nolint:mnd
		y := days / 365             //nolint:mnd
		rem := days % 365           //nolint:mnd
		if rem == 0 {
			return fmt.Sprintf("%dy", y)
		}
		return fmt.Sprintf("%dy %dd", y, rem)
	}
}

func renderStatusSections(w io.Writer, sections []statusSection) {
	sectionStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("4"))       //nolint:mnd
	headerStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("245")).PaddingRight(2) //nolint:mnd
	dataStyle := lipgloss.NewStyle().PaddingRight(2)                                     //nolint:mnd

	for i, sec := range sections {
		if i > 0 {
			fmt.Fprintln(w)
		}
		fmt.Fprintln(w, sectionStyle.Render(sec.title))

		t := table.New().
			Border(lipgloss.HiddenBorder()).
			BorderTop(false).
			BorderBottom(false).
			BorderLeft(false).
			BorderRight(false).
			BorderHeader(false).
			BorderColumn(false).
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
	var label string
	var color string
	switch m.GetHealth() {
	case controlv1.HealthStatus_HEALTH_STATUS_HEALTHY:
		label = "HEALTHY"
		color = "2" //nolint:mnd
	case controlv1.HealthStatus_HEALTH_STATUS_DEGRADED:
		label = "DEGRADED"
		color = "3" //nolint:mnd
	case controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY:
		label = "UNHEALTHY"
		color = "1"
	default:
		return
	}
	fmt.Fprintln(w, lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(color)).Render(label))
	fmt.Fprintln(w)
}

func renderMetricsDetails(w io.Writer, m *controlv1.GetMetricsResponse) {
	fmt.Fprintln(w, lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("4")).Render("DIAGNOSTICS")) //nolint:mnd
	fmt.Fprintf(w, "  vivaldi error:  %.3f\n", m.GetVivaldiError())
	if m.GetEventsApplied() > 0 {
		fmt.Fprintf(w, "  gossip stale:   %.1f%% (%d/%d)\n", m.GetStaleRatio()*100, m.GetEventsStale(), m.GetEventsApplied()) //nolint:mnd
	}
	if m.GetPunchAttempts() > 0 {
		fmt.Fprintf(w, "  punch success:  %d/%d\n", m.GetPunchAttempts()-m.GetPunchFailures(), m.GetPunchAttempts())
	}
	if m.GetCertRenewals() > 0 || m.GetCertRenewalsFailed() > 0 {
		fmt.Fprintf(w, "  cert renewals:  %d ok, %d failed\n", m.GetCertRenewals(), m.GetCertRenewalsFailed())
	}
	fmt.Fprintln(w)
}

func socketPermissionDenied(cmd *cobra.Command) bool {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return false
	}
	_, statErr := os.Stat(filepath.Join(pollenDir, socketName))
	return errors.Is(statErr, os.ErrPermission)
}
