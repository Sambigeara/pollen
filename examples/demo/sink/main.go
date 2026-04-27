// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// sink is a tiny HTTP listener that displays a live RPS counter in a tidy
// terminal pane. The operator exposes it to the mesh with `pln serve <port>
// sink`, after which it's reachable as pln://service/sink.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/charmbracelet/lipgloss"
)

const (
	defaultPort       = 8090
	readHeaderTimeout = 5 * time.Second
	tickInterval      = 250 * time.Millisecond
	sampleInterval    = time.Second
	historySize       = 60
	minPort           = 1
	maxPort           = 65535
	labelWidth        = 8
	thousandsGroup    = 3
	secondsPerMinute  = 60
	minutesPerHour    = 60
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sink:", err)
		os.Exit(1)
	}
}

func run() error {
	port := defaultPort
	if v := os.Getenv("SINK_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < minPort || p > maxPort {
			return errors.New("invalid SINK_PORT (must be 1..65535)")
		}
		port = p
	}

	rootCtx, rootCancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer rootCancel()

	lc := net.ListenConfig{}
	ln, err := lc.Listen(rootCtx, "tcp", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	var counter atomic.Uint64
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		counter.Add(1)
		fmt.Fprint(w, "ok")
	})

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: readHeaderTimeout}

	srvErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
	}()

	tuiDone := make(chan struct{})
	go func() {
		runTUI(rootCtx, port, &counter)
		close(tuiDone)
	}()

	select {
	case <-rootCtx.Done():
	case err := <-srvErr:
		fmt.Fprintln(os.Stderr, "\nserver error:", err)
	}

	_ = srv.Shutdown(context.Background())
	<-tuiDone
	return nil
}

func runTUI(ctx context.Context, port int, counter *atomic.Uint64) {
	hist := newHistory(historySize)
	startedAt := time.Now()
	lastCount := uint64(0)
	lastSampleAt := startedAt

	tick := time.NewTicker(tickInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Fprint(os.Stdout, "\n")
			return
		case now := <-tick.C:
			cur := counter.Load()
			if now.Sub(lastSampleAt) >= sampleInterval {
				hist.push(cur - lastCount)
				lastCount = cur
				lastSampleAt = now
			}
			render(port, cur, hist.last(), time.Since(startedAt), hist)
		}
	}
}

type history struct {
	data []uint64
	max  int
}

func newHistory(size int) *history { return &history{data: make([]uint64, 0, size), max: size} }

func (h *history) push(v uint64) {
	if len(h.data) >= h.max {
		copy(h.data, h.data[1:])
		h.data = h.data[:h.max-1]
	}
	h.data = append(h.data, v)
}

func (h *history) last() uint64 {
	if len(h.data) == 0 {
		return 0
	}
	return h.data[len(h.data)-1]
}

var (
	titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("220"))
	subStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	labelStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Width(labelWidth)
	valueStyle = lipgloss.NewStyle().Bold(true)
	sparkStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("220"))
)

const sparkChars = "▁▂▃▄▅▆▇█"

func render(port int, total, rps uint64, uptime time.Duration, h *history) {
	out := os.Stdout
	fmt.Fprint(out, "\033[H\033[2J")

	fmt.Fprintln(out, titleStyle.Render("sink"))
	fmt.Fprintln(out, subStyle.Render(fmt.Sprintf("listening on :%d", port)))
	fmt.Fprintln(out)
	fmt.Fprintf(out, "  %s%s\n", labelStyle.Render("rps"), valueStyle.Render(formatNum(rps)))
	fmt.Fprintf(out, "  %s%s\n", labelStyle.Render("total"), valueStyle.Render(formatNum(total)))
	fmt.Fprintf(out, "  %s%s\n", labelStyle.Render("uptime"), valueStyle.Render(humanDuration(uptime)))
	fmt.Fprintln(out)
	fmt.Fprintln(out, "  "+sparkStyle.Render(sparkline(h.data, h.max)))
	axis := "  60s" + strings.Repeat(" ", h.max-len("60s")-len("now")) + "now"
	fmt.Fprintln(out, subStyle.Render(axis))
}

func formatNum(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= thousandsGroup {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + len(s)/thousandsGroup)
	pre := len(s) % thousandsGroup
	if pre == 0 {
		pre = thousandsGroup
	}
	b.WriteString(s[:pre])
	for i := pre; i < len(s); i += thousandsGroup {
		b.WriteByte(',')
		b.WriteString(s[i : i+thousandsGroup])
	}
	return b.String()
}

func humanDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%secondsPerMinute)
	default:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%minutesPerHour)
	}
}

// sparkline renders data as block characters scaled to the largest value seen,
// right-aligned within a fixed-width strip so the line grows from the right.
func sparkline(data []uint64, width int) string {
	out := make([]rune, 0, width)
	for i := 0; i < width-len(data); i++ {
		out = append(out, ' ')
	}
	if len(data) == 0 {
		return string(out)
	}
	var maxV uint64
	for _, v := range data {
		if v > maxV {
			maxV = v
		}
	}
	if maxV == 0 {
		for range data {
			out = append(out, ' ')
		}
		return string(out)
	}
	steps := []rune(sparkChars)
	for _, v := range data {
		idx := int(v * uint64(len(steps)-1) / maxV)
		out = append(out, steps[idx])
	}
	return string(out)
}
