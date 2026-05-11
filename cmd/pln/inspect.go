// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/charmbracelet/lipgloss"
	units "github.com/docker/go-units"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

func newInspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <target>",
		Short: "Show detail for a node or resource",
		Long: `Resolves <target> against peers, workloads, static sites, blobs, and
services in this node's view, and prints a structured detail page for
the single match. Use a peer-id hex prefix, workload/site name, blob
name or hash prefix, or a service name (suffixed when ambiguous).`,
		Example: "  pln inspect ab12cd34            # peer by id prefix\n" +
			"  pln inspect greeter             # workload by name\n" +
			"  pln inspect api-ab12            # service with publisher disambiguation",
		Args: cobra.ExactArgs(1),
		RunE: withEnv(runInspect),
	}
}

func runInspect(cmd *cobra.Command, args []string, env *cliEnv) error {
	arg := args[0]

	statusResp, err := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		return err
	}
	st := statusResp.Msg

	target, err := resolveInspectTarget(st, arg)
	if err != nil {
		return err
	}

	resp, err := env.client.Inspect(cmd.Context(), connect.NewRequest(target))
	if err != nil {
		if connect.CodeOf(err) == connect.CodeUnimplemented {
			return errors.New("resource inspection not yet implemented; use `pln status` to see resource details")
		}
		return err
	}

	switch d := resp.Msg.GetDetail().(type) {
	case *controlv1.InspectResponse_Node:
		renderNodeDetail(cmd.OutOrStdout(), d.Node, st)
		return nil
	case nil:
		return errors.New("empty inspect response")
	}
	return fmt.Errorf("unknown inspect detail type %T", resp.Msg.GetDetail())
}

// resolveInspectTarget disambiguates arg against the cluster view. Within
// a kind (e.g. two blobs sharing a hash prefix) the underlying matcher's
// ambiguity error is surfaced immediately. Cross-kind collisions list
// each kind so the operator can disambiguate.
func resolveInspectTarget(st *controlv1.GetStatusResponse, arg string) (*controlv1.InspectRequest, error) {
	var (
		matchedPeer  []byte
		matchedWL    *controlv1.WorkloadSummary
		matchedStat  *controlv1.StaticSummary
		matchedBlob  string
		matchedSvc   *controlv1.ServiceSummary
		descriptions []string
	)

	peer, err := matchPeerArg(st, arg)
	if err != nil {
		return nil, err
	}
	if peer != nil {
		matchedPeer = peer
		descriptions = append(descriptions, "peer "+formatPeerID(peer, false))
	}
	wl, wlErr := matchWorkloadArg(st.GetWorkloads(), arg)
	if wlErr != nil {
		return nil, wlErr
	}
	if wl != nil {
		matchedWL = wl
		descriptions = append(descriptions, "workload "+wl.GetName())
	}
	if site := matchStaticArg(st.GetSites(), arg); site != nil {
		matchedStat = site
		descriptions = append(descriptions, "static site "+site.GetName())
	}
	hash, blobErr := matchBlobArg(st.GetBlobs(), arg)
	switch {
	case blobErr == nil:
		matchedBlob = hash
		descriptions = append(descriptions, "blob "+hash[:shortHexLen])
	case isBlobAmbiguous(blobErr):
		return nil, blobErr
	}
	svc, svcErr := matchServiceArg(st, arg)
	if svcErr != nil {
		return nil, svcErr
	}
	if svc != nil {
		matchedSvc = svc
		descriptions = append(descriptions, "service "+svc.GetName())
	}

	if len(descriptions) == 0 {
		return nil, notFoundErr("no peer or resource matching %q", arg)
	}
	if len(descriptions) > 1 {
		return nil, ambiguousErr("%q matches multiple kinds: %s — use a more specific identifier", arg, strings.Join(descriptions, ", "))
	}

	switch {
	case matchedPeer != nil:
		return &controlv1.InspectRequest{Target: &controlv1.InspectRequest_NodePub{NodePub: matchedPeer}}, nil
	case matchedWL != nil:
		return &controlv1.InspectRequest{Target: &controlv1.InspectRequest_WorkloadHash{WorkloadHash: matchedWL.GetHash()}}, nil
	case matchedStat != nil:
		return &controlv1.InspectRequest{Target: &controlv1.InspectRequest_StaticName{StaticName: matchedStat.GetName()}}, nil
	case matchedBlob != "":
		return &controlv1.InspectRequest{Target: &controlv1.InspectRequest_BlobHash{BlobHash: matchedBlob}}, nil
	case matchedSvc != nil:
		return &controlv1.InspectRequest{Target: &controlv1.InspectRequest_Service{Service: &controlv1.InspectServiceTarget{
			Name:        matchedSvc.GetName(),
			ProviderPub: matchedSvc.GetProvider().GetPeerPub(),
		}}}, nil
	}
	return nil, notFoundErr("no peer or resource matching %q", arg)
}

// matchPeerArg returns a single peer match by hex prefix. A multi-peer
// collision is returned as ambiguousErr so the operator sees the real
// failure mode instead of a misleading "not found".
func matchPeerArg(st *controlv1.GetStatusResponse, arg string) ([]byte, error) {
	lower := strings.ToLower(arg)
	if !isHexPrefix(lower) {
		return nil, nil
	}
	var matches [][]byte
	if self := st.GetSelf(); self != nil && peerIDHasPrefix(self.GetNode().GetPeerPub(), lower) {
		matches = append(matches, self.GetNode().GetPeerPub())
	}
	for _, n := range st.GetNodes() {
		if peerIDHasPrefix(n.GetNode().GetPeerPub(), lower) {
			matches = append(matches, n.GetNode().GetPeerPub())
		}
	}
	switch len(matches) {
	case 0:
		return nil, nil
	case 1:
		return matches[0], nil
	default:
		return nil, ambiguousErr("peer prefix %q matches %d peers — use a longer prefix", arg, len(matches))
	}
}

// matchServiceArg returns a single service match by name (with optional
// publisher suffix). Multi-provider collisions return the collision
// error; "not found" returns nil, nil so the caller can fall through to
// other kinds.
func matchServiceArg(st *controlv1.GetStatusResponse, arg string) (*controlv1.ServiceSummary, error) {
	svc, err := resolveService(st, arg, "")
	if err != nil {
		if isServiceNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return svc, nil
}

// isBlobAmbiguous reports whether err from matchBlobArg indicates a
// within-kind collision (multiple blobs share the name or hash prefix)
// rather than a plain "no match". Detected via the exit code that
// matchBlobArg attaches to ambiguity errors.
func isBlobAmbiguous(err error) bool {
	var ee *exitError
	return errors.As(err, &ee) && ee.code == exitAmbiguous
}

// isServiceNotFound reports whether err from resolveService is a plain
// "no reachable service" / suffix-miss outcome. resolveService threads
// errNoSuffixMatch through wrapErrNoMatch-style errors, and bare
// notFoundErr otherwise; both are recoverable for inspect.
func isServiceNotFound(err error) bool {
	if errors.Is(err, errNoSuffixMatch) {
		return true
	}
	var ee *exitError
	return errors.As(err, &ee) && ee.code == exitNotFound
}

const (
	inspectKeyWidth     = 14
	inspectAttrIndent   = 2
	inspectAttrKeyWidth = inspectKeyWidth - inspectAttrIndent
)

func renderNodeDetail(w io.Writer, detail *controlv1.NodeDetail, st *controlv1.GetStatusResponse) {
	nameLabels := nodeNameLabels(st.GetSelf(), st.GetNodes(), false)

	summary := detail.GetSummary()
	pk := peerKeyString(summary.GetNode().GetPeerPub())
	isSelf := st.GetSelf() != nil && peerKeyString(st.GetSelf().GetNode().GetPeerPub()) == pk

	renderSection(w, "NODE")
	if name := summary.GetName(); name != "" {
		renderKV(w, "name", name)
	}
	renderKV(w, "peer-id", pk)
	renderKV(w, "status", inspectStatusLabel(summary, isSelf))
	if addr := summary.GetAddr(); addr != "" {
		renderKV(w, "addr", addr)
	}
	if cert := detail.GetCert(); cert != nil {
		renderKV(w, "tier", inspectTierChip(cert.GetCanAdmit(), cert.GetCanPublish()))
		if line := inspectMembershipLine(cert); line != "" {
			renderKV(w, "membership", line)
		}
		if line := inspectIssuerLine(detail.GetIssuerChain(), nameLabels); line != "" {
			renderKV(w, "issued by", line)
		}
	}
	if attrs := detail.GetCert().GetAttributes(); attrs != nil && len(attrs.GetFields()) > 0 {
		renderAttributes(w, attrs)
	}

	renderTelemetry(w, summary, detail)
	renderPublished(w, detail)
	fmt.Fprintln(w)
}

func renderTelemetry(w io.Writer, summary *controlv1.NodeSummary, detail *controlv1.NodeDetail) {
	if !hasTelemetry(detail) {
		return
	}
	fmt.Fprintln(w)
	renderSection(w, "TELEMETRY")
	if line := inspectCPULine(summary); line != "" {
		renderKV(w, "cpu", line)
	}
	if line := inspectMemLine(summary, detail.GetMemTotalBytes()); line != "" {
		renderKV(w, "mem", line)
	}
	if summary.GetLatencyMs() > 0 {
		renderKV(w, "latency", fmt.Sprintf("%.1fms", summary.GetLatencyMs()))
	}
	if hasVivaldi(detail) {
		renderKV(w, "vivaldi", fmt.Sprintf("(%.3f, %.3f) height=%.3f error=%.3f", detail.GetVivaldiX(), detail.GetVivaldiY(), detail.GetVivaldiHeight(), detail.GetVivaldiError()))
	}
	if t := detail.GetNatType(); t != "" {
		renderKV(w, "nat", t)
	}
	if n := len(detail.GetReachablePeers()); n > 0 {
		renderKV(w, "reachable", fmt.Sprintf("%d peers", n))
	}
	if summary.GetTunnelCount() > 0 {
		renderKV(w, "tunnels", fmt.Sprintf("%d", summary.GetTunnelCount()))
	}
	if in, out := summary.GetTrafficRateIn(), summary.GetTrafficRateOut(); in > 0 || out > 0 {
		renderKV(w, "traffic", fmt.Sprintf("%s in / %s out", formatRate(in), formatRate(out)))
	}
}

func renderPublished(w io.Writer, detail *controlv1.NodeDetail) {
	if !hasPublished(detail) {
		return
	}
	fmt.Fprintln(w)
	renderSection(w, "PUBLISHES")
	if v := detail.GetPublishedServices(); len(v) > 0 {
		renderKV(w, "services", strings.Join(v, ", "))
	}
	if v := detail.GetPublishedWorkloads(); len(v) > 0 {
		renderKV(w, "workloads", strings.Join(displayHashList(v), ", "))
	}
	if v := detail.GetPublishedStatics(); len(v) > 0 {
		renderKV(w, "statics", strings.Join(v, ", "))
	}
	if v := detail.GetPublishedBlobs(); len(v) > 0 {
		renderKV(w, "blobs", strings.Join(displayHashList(v), ", "))
	}
}

func hasVivaldi(detail *controlv1.NodeDetail) bool {
	return detail.GetVivaldiError() > 0 ||
		detail.GetVivaldiX() != 0 ||
		detail.GetVivaldiY() != 0 ||
		detail.GetVivaldiHeight() != 0
}

var sectionStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("4"))

func renderSection(w io.Writer, title string) {
	fmt.Fprintln(w, sectionStyle.Render(title))
}

func renderKV(w io.Writer, key, value string) {
	fmt.Fprintf(w, "%-*s%s\n", inspectKeyWidth, key, value)
}

func renderAttributes(w io.Writer, attrs *structpb.Struct) {
	keys := make([]string, 0, len(attrs.GetFields()))
	for k := range attrs.GetFields() {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	renderKV(w, "attributes", "")
	for _, k := range keys {
		fmt.Fprintf(w, "  %-*s%s\n", inspectAttrKeyWidth, k, formatStructValue(attrs.GetFields()[k]))
	}
}

func inspectStatusLabel(summary *controlv1.NodeSummary, isSelf bool) string {
	label := formatStatus(summary.GetStatus())
	if isSelf {
		label = "self"
	}
	if summary.GetPubliclyAccessible() {
		label += " (public)"
	}
	return label
}

func inspectTierChip(canAdmit, canPublish bool) string {
	switch tierLabel(canAdmit, canPublish) {
	case tierAdmin:
		return lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("5")).Render("ADMIN")
	case tierPublisher:
		return lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6")).Render("PUBLISHER")
	case tierLeaf:
		return lipgloss.NewStyle().Bold(true).Foreground(mutedFG).Render("LEAF")
	}
	return ""
}

func inspectMembershipLine(cert *controlv1.CertInfo) string {
	if cert.GetNotAfterUnix() == 0 {
		return ""
	}
	exp := time.Unix(cert.GetNotAfterUnix(), 0)
	remaining := time.Until(exp)
	switch {
	case remaining <= 0 || cert.GetHealth() == controlv1.CertHealth_CERT_HEALTH_EXPIRED:
		return "expired"
	default:
		return "expires in " + humanDuration(remaining)
	}
}

func inspectIssuerLine(chain []*controlv1.NodeRef, nameLabels map[string]string) string {
	if len(chain) == 0 {
		return "root (self-issued)"
	}
	labels := make([]string, len(chain))
	for i, ref := range chain {
		labels[i] = inspectPeerLabel(ref.GetPeerPub(), nameLabels)
	}
	if len(labels) == 1 {
		return labels[0]
	}
	return strings.Join(labels, " -> ")
}

func inspectPeerLabel(peerPub []byte, nameLabels map[string]string) string {
	pk := peerKeyString(peerPub)
	if pk == "" {
		return "-"
	}
	if label, ok := nameLabels[pk]; ok && label != "" {
		return label
	}
	if len(pk) > shortHexLen {
		return pk[:shortHexLen]
	}
	return pk
}

func inspectCPULine(summary *controlv1.NodeSummary) string {
	cores := summary.GetNumCpu()
	usage := summary.GetCpuPercent()
	switch {
	case cores == 0 && usage == 0:
		return ""
	case cores == 0:
		return fmt.Sprintf("%d%%", usage)
	case usage == 0:
		return fmt.Sprintf("%d cores", cores)
	default:
		return fmt.Sprintf("%d cores, %d%%", cores, usage)
	}
}

func inspectMemLine(summary *controlv1.NodeSummary, totalBytes uint64) string {
	usage := summary.GetMemPercent()
	switch {
	case totalBytes == 0 && usage == 0:
		return ""
	case totalBytes == 0:
		return fmt.Sprintf("%d%%", usage)
	case usage == 0:
		return units.HumanSize(float64(totalBytes)) + " total"
	default:
		return fmt.Sprintf("%s total, %d%%", units.HumanSize(float64(totalBytes)), usage)
	}
}

func hasTelemetry(detail *controlv1.NodeDetail) bool {
	s := detail.GetSummary()
	if s.GetCpuPercent() > 0 || s.GetMemPercent() > 0 || s.GetNumCpu() > 0 {
		return true
	}
	if s.GetLatencyMs() > 0 || s.GetTunnelCount() > 0 || s.GetTrafficRateIn() > 0 || s.GetTrafficRateOut() > 0 {
		return true
	}
	if detail.GetMemTotalBytes() > 0 || detail.GetNatType() != "" || len(detail.GetReachablePeers()) > 0 {
		return true
	}
	return hasVivaldi(detail)
}

func hasPublished(detail *controlv1.NodeDetail) bool {
	return len(detail.GetPublishedServices()) > 0 ||
		len(detail.GetPublishedWorkloads()) > 0 ||
		len(detail.GetPublishedStatics()) > 0 ||
		len(detail.GetPublishedBlobs()) > 0
}

// displayHashList shortens raw 64-hex entries to shortHexLen but leaves
// named entries (workload/blob names) untouched. Mixed lists from
// PublishedWorkloads/PublishedBlobs commonly carry both.
func displayHashList(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		if len(s) == 64 && isHexPrefix(strings.ToLower(s)) {
			out[i] = s[:shortHexLen]
			continue
		}
		out[i] = s
	}
	return out
}
