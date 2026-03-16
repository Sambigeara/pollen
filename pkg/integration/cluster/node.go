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
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/store"
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
	BootstrapPeers []node.BootstrapPeer
	EnableNATPunch bool
}

// TestNode wraps a full-stack node for integration tests.
type TestNode struct {
	n       *node.Node
	store   *store.Store
	cancel  context.CancelFunc
	peerKey types.PeerKey
	addr    *net.UDPAddr
	name    string
	role    NodeRole
}

// NewTestNode creates and starts a fully wired node backed by VirtualSwitch.
func NewTestNode(t testing.TB, cfg TestNodeConfig) *TestNode { //nolint:thelper
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub := priv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	peerKey := types.PeerKeyFromBytes(pub)

	// Get TLS cert and delegation cert from ClusterAuth.
	_, dc := cfg.Auth.NodeCredentials(priv)

	// Create state store in temp dir.
	tmpDir := t.TempDir()
	stateStore, err := store.Load(tmpDir, pub)
	require.NoError(t, err)
	t.Cleanup(func() { stateStore.Close() })

	// Build auth.NodeCredentials with DelegationSigner.
	creds := &auth.NodeCredentials{
		Trust: cfg.Auth.TrustBundle(),
		Cert:  dc,
		DelegationKey: &auth.DelegationSigner{
			Priv:     priv,
			Trust:    cfg.Auth.TrustBundle(),
			Issuer:   dc,
			Consumed: stateStore,
		},
	}

	// Get VirtualPacketConn from switch.
	vconn := cfg.Switch.Bind(cfg.Addr, cfg.Role)

	// Build node config with injected PacketConn.
	nodeConf := &node.Config{
		Port:             cfg.Addr.Port,
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

	n, err := node.New(nodeConf, priv, creds, stateStore, peer.NewStore(), tmpDir)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(cfg.Context)

	done := make(chan struct{})
	go func() {
		_ = n.Start(ctx)
		close(done)
	}()

	// Cleanup: cancel context, wait for node to stop, then close store.
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second): //nolint:mnd
		}
	})

	select {
	case <-n.Ready():
	case <-time.After(10 * time.Second): //nolint:mnd
		t.Fatal("node did not become ready")
	}

	return &TestNode{
		n:       n,
		store:   stateStore,
		cancel:  cancel,
		peerKey: peerKey,
		addr:    cfg.Addr,
		role:    cfg.Role,
		name:    cfg.Name,
	}
}

func (tn *TestNode) PeerKey() types.PeerKey          { return tn.peerKey }
func (tn *TestNode) Node() *node.Node                { return tn.n }
func (tn *TestNode) Store() *store.Store             { return tn.store }
func (tn *TestNode) Role() NodeRole                  { return tn.role }
func (tn *TestNode) VirtualAddr() *net.UDPAddr       { return tn.addr }
func (tn *TestNode) Name() string                    { return tn.name }
func (tn *TestNode) ConnectedPeers() []types.PeerKey { return tn.n.GetConnectedPeers() }
func (tn *TestNode) ListenPort() int                 { return tn.n.ListenPort() }
func (tn *TestNode) Stop()                           { tn.cancel() }
