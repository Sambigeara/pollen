// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/supervisor"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type TestNodeConfig struct {
	Context        context.Context
	Switch         *VirtualSwitch
	Auth           *ClusterAuth
	Addr           *net.UDPAddr
	Name           string
	Role           NodeRole
	EnableNATPunch bool
}

type TestNode struct {
	n       *supervisor.Supervisor
	cancel  context.CancelFunc
	peerKey types.PeerKey
	addr    *net.UDPAddr
	name    string
	role    NodeRole
	stopped atomic.Bool
}

func NewTestNode(t testing.TB, cfg TestNodeConfig) *TestNode { //nolint:thelper
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub := priv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	peerKey := types.PeerKeyFromBytes(pub)

	_, dc := cfg.Auth.NodeCredentials(priv)

	pollenDir := t.TempDir()
	identityDir := auth.IdentityPath(pollenDir)
	creds := auth.NewNodeCredentials(cfg.Auth.RootPub(), dc)
	require.NoError(t, auth.SaveNodeCredentials(identityDir, creds))
	signer, err := auth.NewDelegationSigner(identityDir, priv)
	require.NoError(t, err)
	creds.SetDelegationKey(signer)

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
		BootstrapPublic:  cfg.Role == Public,
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

	t.Cleanup(func() {
		tn.stopped.Store(true)
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
	if tn.stopped.Load() {
		return nil
	}
	return tn.n.GetConnectedPeers()
}
func (tn *TestNode) ListenPort() int { return tn.n.ListenPort() }
func (tn *TestNode) Stop()           { tn.stopped.Store(true); tn.cancel() }
