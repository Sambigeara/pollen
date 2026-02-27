package main

import (
	"context"
	"fmt"
	"io"
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
		if connect.CodeOf(err) == connect.CodeUnavailable {
			fmt.Fprintln(cmd.OutOrStdout(), "daemon is not running")
		} else {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
		}
		return
	}

	st := resp.Msg
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
		headers: []string{"NODE", "STATUS", "ADDR"},
	}

	if self := st.GetSelf(); self != nil && self.GetNode() != nil {
		label := formatPeerID(self.GetNode().GetPeerId(), opts.wide) + " (self)"
		addr := self.GetAddr()
		if addr == "" {
			addr = "-"
		}
		status := "online"
		if self.GetPubliclyAccessible() {
			status = "online (public)"
		}
		sec.rows = append(sec.rows, []string{label, status, addr})
	}

	filtered := 0
	for _, n := range st.Nodes {
		if !opts.includeAll && !isReachableStatus(n.GetStatus()) {
			filtered++
			continue
		}
		label := formatPeerID(n.GetNode().GetPeerId(), opts.wide)
		status := formatStatus(n.GetStatus())
		if n.GetPubliclyAccessible() && isReachableStatus(n.GetStatus()) {
			status += " (public)"
		}
		addr := n.GetAddr()
		if addr == "" {
			addr = "-"
		}
		sec.rows = append(sec.rows, []string{label, status, addr})
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

	reachableProviders := map[string]bool{}
	if st.GetSelf() != nil && st.GetSelf().GetNode() != nil {
		selfID := peerKeyString(st.GetSelf().GetNode().GetPeerId())
		reachableProviders[selfID] = true
	}
	for _, n := range st.Nodes {
		if isReachableStatus(n.GetStatus()) {
			id := peerKeyString(n.GetNode().GetPeerId())
			reachableProviders[id] = true
		}
	}

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
		sec.rows = append(sec.rows, []string{s.GetName(), provider, fmt.Sprintf("%d", s.GetPort()), local})
	}

	if filtered > 0 {
		sec.footer = fmt.Sprintf("offline services: %d (use --all)", filtered)
	}
	return sec
}

func certExpiryFooter(st *controlv1.GetStatusResponse) string {
	const certExpirySkew = time.Minute

	var latest time.Time
	for _, c := range st.GetCertificates() {
		t := time.Unix(c.GetNotAfterUnix(), 0)
		if t.After(latest) {
			latest = t
		}
	}
	if latest.IsZero() {
		return ""
	}
	remaining := time.Until(latest.Add(certExpirySkew))
	if remaining <= 0 {
		return "membership expired"
	}
	return "membership expires in " + humanDuration(remaining)
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
