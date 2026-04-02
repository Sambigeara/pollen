//go:build integration

package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/supervisor"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestNodeConfig holds parameters for creating a TestNode.
type TestNodeConfig struct {
	Context        context.Context
	Switch         *VirtualSwitch
	Auth           *ClusterAuth
	Addr           *net.UDPAddr
	Name           string
	Role           NodeRole
	BootstrapPeers []supervisor.BootstrapTarget
	EnableNATPunch bool
}

// TestNode wraps a full-stack node for integration tests.
type TestNode struct {
	n       *supervisor.Supervisor
	cancel  context.CancelFunc
	peerKey types.PeerKey
	addr    *net.UDPAddr
	name    string
	role    NodeRole
	stopped bool
}

// NewTestNode creates and starts a fully wired node backed by VirtualSwitch.
func NewTestNode(t testing.TB, cfg TestNodeConfig) *TestNode { //nolint:thelper
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub := priv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	peerKey := types.PeerKeyFromBytes(pub)

	// Get TLS cert and delegation cert from ClusterAuth.
	_, dc := cfg.Auth.NodeCredentials(priv)

	// Persist credentials to disk and load the signer via the production path.
	pollenDir := t.TempDir()
	creds := auth.NewNodeCredentials(cfg.Auth.RootPub(), dc)
	require.NoError(t, auth.SaveNodeCredentials(pollenDir, creds))
	signer, err := auth.NewDelegationSigner(pollenDir, priv, 24*time.Hour) //nolint:mnd
	require.NoError(t, err)
	creds.SetDelegationKey(signer)

	// Get VirtualPacketConn from switch.
	vconn := cfg.Switch.Bind(cfg.Addr, cfg.Role)

	opts := supervisor.Options{
		SigningKey:       priv,
		PollenDir:        pollenDir,
		ListenPort:       cfg.Addr.Port,
		AdvertisedIPs:    []string{cfg.Addr.IP.String()},
		PacketConn:       vconn,
		DisableNATPunch:  !cfg.EnableNATPunch,
		PeerTickInterval: 1 * time.Second,
		GossipInterval:   1 * time.Second,
		TLSIdentityTTL:   24 * time.Hour,  //nolint:mnd
		MembershipTTL:    24 * time.Hour,  //nolint:mnd
		ReconnectWindow:  5 * time.Minute, //nolint:mnd
		BootstrapPeers:   cfg.BootstrapPeers,
	}

	n, err := supervisor.New(opts, creds, auth.NewInviteConsumer(nil))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(cfg.Context)

	done := make(chan struct{})
	go func() {
		_ = n.Run(ctx)
		close(done)
	}()

	select {
	case <-n.Ready():
	case <-time.After(10 * time.Second): //nolint:mnd
		t.Fatal("node did not become ready")
	}

	tn := &TestNode{
		n:       n,
		cancel:  cancel,
		peerKey: peerKey,
		addr:    cfg.Addr,
		role:    cfg.Role,
		name:    cfg.Name,
	}

	// Cleanup: mark stopped, cancel context, wait for node to stop.
	t.Cleanup(func() {
		tn.stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second): //nolint:mnd
		}
	})

	return tn
}

func (tn *TestNode) PeerKey() types.PeerKey       { return tn.peerKey }
func (tn *TestNode) Node() *supervisor.Supervisor { return tn.n }
func (tn *TestNode) Store() state.StateStore      { return tn.n.StateStore() }
func (tn *TestNode) Role() NodeRole               { return tn.role }
func (tn *TestNode) VirtualAddr() *net.UDPAddr    { return tn.addr }
func (tn *TestNode) Name() string                 { return tn.name }
func (tn *TestNode) ConnectedPeers() []types.PeerKey {
	if tn.stopped {
		return nil
	}
	return tn.n.GetConnectedPeers()
}
func (tn *TestNode) ListenPort() int { return tn.n.ListenPort() }
func (tn *TestNode) Stop()           { tn.stopped = true; tn.cancel() }
