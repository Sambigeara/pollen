// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

func TestResolveInspectTarget_Peer(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Self:  &controlv1.NodeSummary{Node: nodeRef("a")},
		Nodes: []*controlv1.NodeSummary{{Node: nodeRef("b")}},
	}
	// Self peer id is hex-encoded raw bytes; the prefix matches the first
	// hex byte ("61" for 'a' in our test helper).
	target, err := resolveInspectTarget(st, "61")
	require.NoError(t, err)
	require.NotNil(t, target.GetNodePub())
}

func TestResolveInspectTarget_Workload(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Workloads: []*controlv1.WorkloadSummary{{Hash: "abc123", Name: "greeter"}},
	}
	target, err := resolveInspectTarget(st, "greeter")
	require.NoError(t, err)
	require.Equal(t, "abc123", target.GetWorkloadHash())
}

func TestResolveInspectTarget_StaticSite(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Sites: []*controlv1.StaticSummary{{Name: "marketing"}},
	}
	target, err := resolveInspectTarget(st, "marketing")
	require.NoError(t, err)
	require.Equal(t, "marketing", target.GetStaticName())
}

func TestResolveInspectTarget_Blob(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Blobs: []*controlv1.BlobSummary{{Hash: "deadbeef", Name: "payload", Publisher: nodeRef("a")}},
	}
	target, err := resolveInspectTarget(st, "payload")
	require.NoError(t, err)
	require.Equal(t, "deadbeef", target.GetBlobHash())
}

func TestResolveInspectTarget_Service(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{Node: nodeRef("a"), Status: controlv1.NodeStatus_NODE_STATUS_ONLINE},
		Services: []*controlv1.ServiceSummary{
			{Name: "api", Provider: nodeRef("a"), Port: 8080},
		},
	}
	target, err := resolveInspectTarget(st, "api")
	require.NoError(t, err)
	svc := target.GetService()
	require.NotNil(t, svc)
	require.Equal(t, "api", svc.GetName())
}

func TestResolveInspectTarget_NotFound(t *testing.T) {
	st := &controlv1.GetStatusResponse{}
	_, err := resolveInspectTarget(st, "nothing-here")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no peer or resource matching")
}

func TestResolveInspectTarget_AmbiguousPeerPrefix(t *testing.T) {
	// Two peers whose hex starts with "61" — the test helper sets the first
	// byte from the rune, so any single hex char hits a single peer; we use
	// matching first bytes to force a collision.
	peerA := &controlv1.NodeSummary{Node: nodeRef("a")}
	peerB := &controlv1.NodeSummary{Node: &controlv1.NodeRef{PeerPub: append([]byte{0x61, 0x02}, make([]byte, 30)...)}}
	st := &controlv1.GetStatusResponse{Self: peerA, Nodes: []*controlv1.NodeSummary{peerB}}

	_, err := resolveInspectTarget(st, "61")
	require.Error(t, err)
	require.Contains(t, err.Error(), "peer prefix")
}

func TestResolveInspectTarget_AmbiguousWorkloadHashPrefix(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Workloads: []*controlv1.WorkloadSummary{
			{Hash: "abc1deadbeef", Name: "greeter"},
			{Hash: "abc2deadbeef", Name: "classifier"},
		},
	}
	_, err := resolveInspectTarget(st, "abc")
	require.Error(t, err)
	require.Contains(t, err.Error(), "multiple workloads")
}

func TestResolveInspectTarget_AmbiguousBlobHashPrefix(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Blobs: []*controlv1.BlobSummary{
			{Hash: "abc1deadbeef", Publisher: nodeRef("a")},
			{Hash: "abc2deadbeef", Publisher: nodeRef("b")},
		},
	}
	_, err := resolveInspectTarget(st, "abc")
	require.Error(t, err)
	require.Contains(t, err.Error(), "multiple blobs")
}

func TestResolveInspectTarget_AmbiguousCrossKind(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Sites:     []*controlv1.StaticSummary{{Name: "foo"}},
		Workloads: []*controlv1.WorkloadSummary{{Hash: "abc", Name: "foo"}},
	}
	_, err := resolveInspectTarget(st, "foo")
	require.Error(t, err)
	require.Contains(t, err.Error(), "matches multiple kinds")
}

func TestRenderNodeDetail_BasicAdmin(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{Node: nodeRef("a"), Name: "alpha"},
	}
	detail := &controlv1.NodeDetail{
		Summary: &controlv1.NodeSummary{
			Node:               nodeRef("a"),
			Name:               "alpha",
			Status:             controlv1.NodeStatus_NODE_STATUS_ONLINE,
			Addr:               "10.0.0.1:7531",
			CpuPercent:         12,
			MemPercent:         24,
			NumCpu:             8,
			PubliclyAccessible: true,
		},
		Cert: &controlv1.CertInfo{
			CanAdmit:     true,
			CanPublish:   true,
			CanDelegate:  true,
			NotAfterUnix: nowPlus(t, 23),
			Attributes:   mustStruct(t, map[string]any{"role": "primary"}),
		},
		MemTotalBytes:      8 * 1024 * 1024 * 1024,
		NatType:            "easy",
		PublishedServices:  []string{"api", "dns"},
		PublishedWorkloads: []string{"greeter"},
	}

	var buf bytes.Buffer
	renderNodeDetail(&buf, detail, st)

	out := stripANSI(buf.String())
	require.Contains(t, out, "NODE")
	require.Contains(t, out, "alpha")
	require.Contains(t, out, "ADMIN")
	require.Contains(t, out, "self")
	require.Contains(t, out, "(public)")
	require.Contains(t, out, "role")
	require.Contains(t, out, "primary")
	require.Contains(t, out, "TELEMETRY")
	require.Contains(t, out, "8 cores, 12%")
	require.Contains(t, out, "easy")
	require.Contains(t, out, "PUBLISHES")
	require.Contains(t, out, "api, dns")
	require.Contains(t, out, "greeter")
}

func TestRenderNodeDetail_LeafMinimal(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Nodes: []*controlv1.NodeSummary{{Node: nodeRef("b")}},
	}
	detail := &controlv1.NodeDetail{
		Summary: &controlv1.NodeSummary{Node: nodeRef("b"), Status: controlv1.NodeStatus_NODE_STATUS_OFFLINE},
		Cert: &controlv1.CertInfo{
			NotAfterUnix: nowPlus(t, 1),
		},
	}

	var buf bytes.Buffer
	renderNodeDetail(&buf, detail, st)

	out := stripANSI(buf.String())
	require.Contains(t, out, "LEAF")
	require.Contains(t, out, "offline")
	require.NotContains(t, out, "TELEMETRY")
	require.NotContains(t, out, "PUBLISHES")
}

func TestRenderNodeDetail_ExpiredCert(t *testing.T) {
	st := &controlv1.GetStatusResponse{Nodes: []*controlv1.NodeSummary{{Node: nodeRef("b")}}}
	detail := &controlv1.NodeDetail{
		Summary: &controlv1.NodeSummary{Node: nodeRef("b")},
		Cert: &controlv1.CertInfo{
			Health:       controlv1.CertHealth_CERT_HEALTH_EXPIRED,
			NotAfterUnix: 1,
		},
	}
	var buf bytes.Buffer
	renderNodeDetail(&buf, detail, st)
	require.Contains(t, stripANSI(buf.String()), "expired")
}

func TestDisplayHashList(t *testing.T) {
	in := []string{
		"abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
		"greeter",
	}
	out := displayHashList(in)
	require.Equal(t, "abcdefab", out[0])
	require.Equal(t, "greeter", out[1])
}

func TestInspectStatusLabel(t *testing.T) {
	cases := []struct {
		name   string
		status controlv1.NodeStatus
		self   bool
		public bool
		want   string
	}{
		{"self", controlv1.NodeStatus_NODE_STATUS_ONLINE, true, false, "self"},
		{"self_public", controlv1.NodeStatus_NODE_STATUS_ONLINE, true, true, "self (public)"},
		{"direct", controlv1.NodeStatus_NODE_STATUS_ONLINE, false, false, "direct"},
		{"indirect_public", controlv1.NodeStatus_NODE_STATUS_INDIRECT, false, true, "indirect (public)"},
		{"offline", controlv1.NodeStatus_NODE_STATUS_OFFLINE, false, false, "offline"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &controlv1.NodeSummary{Status: tc.status, PubliclyAccessible: tc.public}
			require.Equal(t, tc.want, inspectStatusLabel(s, tc.self))
		})
	}
}

func TestInspectIssuerLine(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		require.Equal(t, "root (self-issued)", inspectIssuerLine(nil, nil))
	})
	t.Run("single_issuer_named", func(t *testing.T) {
		issuer := nodeRef("a")
		pk := peerKeyString(issuer.GetPeerPub())
		labels := map[string]string{pk: "root-admin"}
		require.Equal(t, "root-admin", inspectIssuerLine([]*controlv1.NodeRef{issuer}, labels))
	})
	t.Run("multi_hop", func(t *testing.T) {
		got := inspectIssuerLine([]*controlv1.NodeRef{nodeRef("a"), nodeRef("b")}, nil)
		require.Contains(t, got, "->")
	})
}

func nowPlus(t *testing.T, hours int) int64 {
	t.Helper()
	return 1_700_000_000 + int64(hours)*3600
}

func mustStruct(t *testing.T, v map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(v)
	require.NoError(t, err)
	return s
}

// stripANSI removes lipgloss colour codes so substring assertions on
// rendered output stay readable. Anything starting with ESC '[' through
// the next 'm' is treated as a control sequence.
func stripANSI(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			j := i + 2
			for j < len(s) && s[j] != 'm' {
				j++
			}
			if j < len(s) {
				i = j + 1
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}
