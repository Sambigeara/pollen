package main

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

func newStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status [nodes|services|connections]",
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
		fmt.Fprintln(cmd.ErrOrStderr(), err)
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
		if s := collectConnectionsSection(st, opts); len(s.rows) > 0 {
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
	case "connections", "connection", "connected":
		if s := collectConnectionsSection(st, opts); len(s.rows) > 0 {
			sections = append(sections, s)
		}
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unknown status selector %q (use: nodes|services|connections)\n", mode)
		return
	}
	renderStatusSections(cmd.OutOrStdout(), sections)
}

type statusViewOpts struct {
	wide       bool
	includeAll bool
}

type statusSection struct {
	title   string
	headers []string
	rows    [][]string
	footer  string
}

func collectPeersSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "PEERS",
		headers: []string{"NODE", "STATUS", "ADDR"},
	}

	if self := st.GetSelf(); self != nil && self.GetNode() != nil {
		peer := formatPeerID(self.GetNode().GetPeerId(), opts.wide) + " (self)"
		addr := self.GetAddr()
		if addr == "" {
			addr = "-"
		}
		sec.rows = append(sec.rows, []string{peer, "online", addr})
	}

	filtered := 0
	for _, n := range st.Nodes {
		if !opts.includeAll && !isReachableStatus(n.GetStatus()) {
			filtered++
			continue
		}
		peer := formatPeerID(n.GetNode().GetPeerId(), opts.wide)
		status := formatStatus(n.GetStatus())
		addr := n.GetAddr()
		if addr == "" {
			addr = "-"
		}
		sec.rows = append(sec.rows, []string{peer, status, addr})
	}

	if filtered > 0 {
		sec.footer = fmt.Sprintf("offline peers: %d (use --all)", filtered)
	}
	return sec
}

func collectServicesSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "SERVICES",
		headers: []string{"SERVICE", "PROVIDER", "PORT"},
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

	filtered := 0
	for _, s := range st.Services {
		providerID := peerKeyString(s.GetProvider().GetPeerId())
		if !opts.includeAll && !reachableProviders[providerID] {
			filtered++
			continue
		}
		provider := formatPeerID(s.GetProvider().GetPeerId(), opts.wide)
		sec.rows = append(sec.rows, []string{s.GetName(), provider, fmt.Sprintf("%d", s.GetPort())})
	}

	if filtered > 0 {
		sec.footer = fmt.Sprintf("offline services: %d (use --all)", filtered)
	}
	return sec
}

func collectConnectionsSection(st *controlv1.GetStatusResponse, opts statusViewOpts) statusSection {
	sec := statusSection{
		title:   "CONNECTED",
		headers: []string{"LOCAL", "SERVICE", "PROVIDER", "REMOTE"},
	}

	for _, c := range st.Connections {
		service := c.GetServiceName()
		if service == "" {
			service = strconv.FormatUint(uint64(c.GetRemotePort()), 10)
		}
		provider := formatPeerID(c.GetPeer().GetPeerId(), opts.wide)
		sec.rows = append(sec.rows, []string{
			fmt.Sprintf("%d", c.GetLocalPort()), service, provider, fmt.Sprintf("%d", c.GetRemotePort()),
		})
	}
	return sec
}

const (
	statusRowSection = iota
	statusRowHeader
	statusRowData
	statusRowSpacer
)

func renderStatusSections(w io.Writer, sections []statusSection) {
	maxCols := 0
	for _, sec := range sections {
		if len(sec.headers) > maxCols {
			maxCols = len(sec.headers)
		}
		for _, row := range sec.rows {
			if len(row) > maxCols {
				maxCols = len(row)
			}
		}
	}
	if maxCols == 0 {
		return
	}

	var rowKinds []int
	padRow := func(src []string) []string {
		row := make([]string, maxCols)
		copy(row, src)
		return row
	}

	t := table.New().
		Border(lipgloss.HiddenBorder()).
		BorderTop(false).
		BorderBottom(false).
		BorderLeft(false).
		BorderRight(false).
		BorderHeader(false).
		BorderColumn(false)

	for i, sec := range sections {
		if i > 0 {
			t.Row(padRow(nil)...)
			rowKinds = append(rowKinds, statusRowSpacer)
		}
		t.Row(padRow([]string{sec.title})...)
		rowKinds = append(rowKinds, statusRowSection)
		t.Row(padRow(sec.headers)...)
		rowKinds = append(rowKinds, statusRowHeader)
		for _, dataRow := range sec.rows {
			t.Row(padRow(dataRow)...)
			rowKinds = append(rowKinds, statusRowData)
		}
	}

	sectionStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("4")).PaddingRight(2)
	headerStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("245")).PaddingRight(2)
	dataStyle := lipgloss.NewStyle().PaddingRight(2)

	t.StyleFunc(func(row, col int) lipgloss.Style {
		if row < 0 || row >= len(rowKinds) {
			return dataStyle
		}
		switch rowKinds[row] {
		case statusRowSection:
			return sectionStyle
		case statusRowHeader:
			return headerStyle
		default:
			return dataStyle
		}
	})

	fmt.Fprintln(w, t)

	for _, sec := range sections {
		if sec.footer != "" {
			fmt.Fprintln(w)
			fmt.Fprintln(w, sec.footer)
		}
	}
	fmt.Fprintln(w)
}
