package main

import (
	"encoding/hex"
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/stretchr/testify/require"
)

// fakePeerID returns a 32-byte slice whose hex encoding starts with the given prefix.
func fakePeerID(hexPrefix string) []byte {
	full := hexPrefix
	for len(full) < 64 {
		full += "0"
	}
	b, _ := hex.DecodeString(full)
	return b
}

func TestCertExpiryFooterWithinSkew(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Certificates: []*controlv1.CertInfo{{
			NotAfterUnix: time.Now().Add(-30 * time.Second).Unix(),
		}},
	}

	footer := certExpiryFooter(st)
	require.Contains(t, footer, "membership expires in ")
}

func TestCertExpiryFooterBeyondSkew(t *testing.T) {
	st := &controlv1.GetStatusResponse{
		Certificates: []*controlv1.CertInfo{{
			NotAfterUnix: time.Now().Add(-2 * time.Minute).Unix(),
			Health:       controlv1.CertHealth_CERT_HEALTH_EXPIRED,
		}},
	}

	footer := certExpiryFooter(st)
	require.Contains(t, footer, "membership expired")
}

func TestMinUniquePrefixes(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "already unique first char",
			in:   []string{"ab12", "cd34"},
			want: []string{"a", "c"},
		},
		{
			name: "shared prefix needs two chars",
			in:   []string{"ab12", "ac34"},
			want: []string{"ab", "ac"},
		},
		{
			name: "three-way collision",
			in:   []string{"abc1", "abd2", "xyz9"},
			want: []string{"abc", "abd", "x"},
		},
		{
			name: "identical strings use full length",
			in:   []string{"aaa", "aaa"},
			want: []string{"aaa", "aaa"},
		},
		{
			name: "single entry",
			in:   []string{"hello"},
			want: []string{"h"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := minUniquePrefixes(tt.in)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestServiceNameSuffixes(t *testing.T) {
	peerA := fakePeerID("aa")
	peerB := fakePeerID("bb")
	peerC := fakePeerID("cc")

	services := []*controlv1.ServiceSummary{
		{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerA}, Port: 8080},
		{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerB}, Port: 8080},
		{Name: "dns", Provider: &controlv1.NodeRef{PeerId: peerC}, Port: 53},
	}
	includeAll := func(string) bool { return true }

	suffixes := serviceNameSuffixes(services, includeAll)

	pkA := peerKeyString(peerA)
	pkB := peerKeyString(peerB)
	pkC := peerKeyString(peerC)

	require.NotEmpty(t, suffixes[serviceProviderKey{"http", pkA}])
	require.NotEmpty(t, suffixes[serviceProviderKey{"http", pkB}])
	require.NotEqual(t, suffixes[serviceProviderKey{"http", pkA}], suffixes[serviceProviderKey{"http", pkB}])

	// Non-colliding name has no suffix.
	require.Empty(t, suffixes[serviceProviderKey{"dns", pkC}])
}

func TestServiceNameSuffixesFilterExcluded(t *testing.T) {
	peerA := fakePeerID("aa")
	peerB := fakePeerID("bb")

	services := []*controlv1.ServiceSummary{
		{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerA}, Port: 8080},
		{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerB}, Port: 8080},
	}

	pkA := peerKeyString(peerA)
	onlyA := func(pk string) bool { return pk == pkA }

	suffixes := serviceNameSuffixes(services, onlyA)
	require.Empty(t, suffixes)
}

func TestCollectServicesSectionCollision(t *testing.T) {
	peerA := fakePeerID("aa")
	peerB := fakePeerID("bb")

	st := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{Node: &controlv1.NodeRef{PeerId: peerA}},
		Services: []*controlv1.ServiceSummary{
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerA}, Port: 8080},
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerB}, Port: 8080},
		},
		Nodes: []*controlv1.NodeSummary{
			{Node: &controlv1.NodeRef{PeerId: peerB}, Status: controlv1.NodeStatus_NODE_STATUS_ONLINE},
		},
	}

	sec := collectServicesSection(st, statusViewOpts{})
	require.Len(t, sec.rows, 2)

	// Both rows should have suffixed names.
	require.Contains(t, sec.rows[0][0], "http-")
	require.Contains(t, sec.rows[1][0], "http-")
	require.NotEqual(t, sec.rows[0][0], sec.rows[1][0])
}

func TestCollectServicesSectionNoCollision(t *testing.T) {
	peerA := fakePeerID("aa")

	st := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{Node: &controlv1.NodeRef{PeerId: peerA}},
		Services: []*controlv1.ServiceSummary{
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerA}, Port: 8080},
		},
	}

	sec := collectServicesSection(st, statusViewOpts{})
	require.Len(t, sec.rows, 1)
	require.Equal(t, "http", sec.rows[0][0])
}

func TestResolveServiceSuffixArg(t *testing.T) {
	peerA := fakePeerID("aa")
	peerB := fakePeerID("bb")

	st := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{Node: &controlv1.NodeRef{PeerId: peerA}},
		Services: []*controlv1.ServiceSummary{
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerA}, Port: 8080},
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerB}, Port: 8080},
		},
		Nodes: []*controlv1.NodeSummary{
			{Node: &controlv1.NodeRef{PeerId: peerB}, Status: controlv1.NodeStatus_NODE_STATUS_ONLINE},
		},
	}

	pkA := peerKeyString(peerA)
	// Use the first two chars of pkA as the suffix.
	svc, err := resolveService(st, "http-"+pkA[:2], "")
	require.NoError(t, err)
	require.Equal(t, peerA, svc.GetProvider().GetPeerId())
}

func TestResolveServiceCollisionError(t *testing.T) {
	peerA := fakePeerID("aa")
	peerB := fakePeerID("bb")

	st := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{Node: &controlv1.NodeRef{PeerId: peerA}},
		Services: []*controlv1.ServiceSummary{
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerA}, Port: 8080},
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerB}, Port: 8080},
		},
		Nodes: []*controlv1.NodeSummary{
			{Node: &controlv1.NodeRef{PeerId: peerB}, Status: controlv1.NodeStatus_NODE_STATUS_ONLINE},
		},
	}

	_, err := resolveService(st, "http", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pln connect http-a")
	require.Contains(t, err.Error(), "pln connect http-b")
	require.Contains(t, err.Error(), "pick one")
}

func TestResolveServiceDashedNameSuffix(t *testing.T) {
	peerA := fakePeerID("aa")
	peerB := fakePeerID("bb")

	st := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{Node: &controlv1.NodeRef{PeerId: peerA}},
		Services: []*controlv1.ServiceSummary{
			{Name: "my-service", Provider: &controlv1.NodeRef{PeerId: peerA}, Port: 8080},
			{Name: "my-service", Provider: &controlv1.NodeRef{PeerId: peerB}, Port: 8080},
		},
		Nodes: []*controlv1.NodeSummary{
			{Node: &controlv1.NodeRef{PeerId: peerB}, Status: controlv1.NodeStatus_NODE_STATUS_ONLINE},
		},
	}

	pkB := peerKeyString(peerB)
	svc, err := resolveService(st, "my-service-"+pkB[:2], "")
	require.NoError(t, err)
	require.Equal(t, peerB, svc.GetProvider().GetPeerId())
}

func TestResolveConnectionSuffixArg(t *testing.T) {
	peerA := fakePeerID("aa")
	peerB := fakePeerID("bb")

	st := &controlv1.GetStatusResponse{
		Connections: []*controlv1.ConnectionSummary{
			{ServiceName: "http", Peer: &controlv1.NodeRef{PeerId: peerA}, RemotePort: 8080, LocalPort: 9090},
			{ServiceName: "http", Peer: &controlv1.NodeRef{PeerId: peerB}, RemotePort: 8080, LocalPort: 9091},
		},
	}

	pkA := peerKeyString(peerA)
	c, err := resolveConnection(st, "http-"+pkA[:2], "")
	require.NoError(t, err)
	require.Equal(t, uint32(9090), c.GetLocalPort())
}

func TestResolveConnectionCollisionError(t *testing.T) {
	peerA := fakePeerID("aa")
	peerB := fakePeerID("bb")

	st := &controlv1.GetStatusResponse{
		Connections: []*controlv1.ConnectionSummary{
			{ServiceName: "http", Peer: &controlv1.NodeRef{PeerId: peerA}, RemotePort: 8080, LocalPort: 9090},
			{ServiceName: "http", Peer: &controlv1.NodeRef{PeerId: peerB}, RemotePort: 8080, LocalPort: 9091},
		},
	}

	_, err := resolveConnection(st, "http", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pln disconnect http-a")
	require.Contains(t, err.Error(), "pln disconnect http-b")
}

func TestResolveConnectionDashedNameSuffix(t *testing.T) {
	peerA := fakePeerID("aa")
	peerB := fakePeerID("bb")

	st := &controlv1.GetStatusResponse{
		Connections: []*controlv1.ConnectionSummary{
			{ServiceName: "my-service", Peer: &controlv1.NodeRef{PeerId: peerA}, RemotePort: 8080, LocalPort: 9090},
			{ServiceName: "my-service", Peer: &controlv1.NodeRef{PeerId: peerB}, RemotePort: 8080, LocalPort: 9091},
		},
	}

	pkB := peerKeyString(peerB)
	c, err := resolveConnection(st, "my-service-"+pkB[:2], "")
	require.NoError(t, err)
	require.Equal(t, uint32(9091), c.GetLocalPort())
}

func TestResolveServiceAmbiguousSuffix(t *testing.T) {
	// Three providers whose hex keys share prefix "a": aa, ab, ac.
	// Typing "http-a" should produce a collision error with suggestions,
	// not the generic "no reachable service" message.
	peerAA := fakePeerID("aa")
	peerAB := fakePeerID("ab")
	peerAC := fakePeerID("ac")

	st := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{Node: &controlv1.NodeRef{PeerId: peerAA}},
		Services: []*controlv1.ServiceSummary{
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerAA}, Port: 8080},
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerAB}, Port: 8080},
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerAC}, Port: 8080},
		},
		Nodes: []*controlv1.NodeSummary{
			{Node: &controlv1.NodeRef{PeerId: peerAB}, Status: controlv1.NodeStatus_NODE_STATUS_ONLINE},
			{Node: &controlv1.NodeRef{PeerId: peerAC}, Status: controlv1.NodeStatus_NODE_STATUS_ONLINE},
		},
	}

	_, err := resolveService(st, "http-a", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pick one")
	require.Contains(t, err.Error(), "pln connect http-aa")
	require.Contains(t, err.Error(), "pln connect http-ab")
	require.Contains(t, err.Error(), "pln connect http-ac")
}

func TestResolveConnectionAmbiguousSuffix(t *testing.T) {
	peerAA := fakePeerID("aa")
	peerAB := fakePeerID("ab")

	st := &controlv1.GetStatusResponse{
		Connections: []*controlv1.ConnectionSummary{
			{ServiceName: "http", Peer: &controlv1.NodeRef{PeerId: peerAA}, RemotePort: 8080, LocalPort: 9090},
			{ServiceName: "http", Peer: &controlv1.NodeRef{PeerId: peerAB}, RemotePort: 8080, LocalPort: 9091},
		},
	}

	_, err := resolveConnection(st, "http-a", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pick one")
	require.Contains(t, err.Error(), "pln disconnect http-aa")
	require.Contains(t, err.Error(), "pln disconnect http-ab")
}

func TestCollectServicesSectionCollisionFooter(t *testing.T) {
	peerA := fakePeerID("aa")
	peerB := fakePeerID("bb")

	st := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{Node: &controlv1.NodeRef{PeerId: peerA}},
		Services: []*controlv1.ServiceSummary{
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerA}, Port: 8080},
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerB}, Port: 8080},
		},
		Nodes: []*controlv1.NodeSummary{
			{Node: &controlv1.NodeRef{PeerId: peerB}, Status: controlv1.NodeStatus_NODE_STATUS_ONLINE},
		},
	}

	sec := collectServicesSection(st, statusViewOpts{})
	require.Contains(t, sec.footer, "service suffixes match the start of the provider ID")
}

func TestCollectServicesSectionNoCollisionNoFooter(t *testing.T) {
	peerA := fakePeerID("aa")

	st := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{Node: &controlv1.NodeRef{PeerId: peerA}},
		Services: []*controlv1.ServiceSummary{
			{Name: "http", Provider: &controlv1.NodeRef{PeerId: peerA}, Port: 8080},
		},
	}

	sec := collectServicesSection(st, statusViewOpts{})
	require.Empty(t, sec.footer)
}
