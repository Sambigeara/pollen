package config

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForgetBootstrapPeer(t *testing.T) {
	keyA := make([]byte, ed25519PublicKeyBytes)
	keyA[0] = 0xAA
	keyB := make([]byte, ed25519PublicKeyBytes)
	keyB[0] = 0xBB

	cfg := &Config{
		BootstrapPeers: []BootstrapPeer{
			{PeerPub: hex.EncodeToString(keyA), Addrs: []string{"relay1.example.com:60611"}},
			{PeerPub: hex.EncodeToString(keyB), Addrs: []string{"relay2.example.com:60611"}},
		},
	}

	cfg.ForgetBootstrapPeer(keyA)

	require.Len(t, cfg.BootstrapPeers, 1)
	require.Equal(t, hex.EncodeToString(keyB), cfg.BootstrapPeers[0].PeerPub)
}

func TestForgetBootstrapPeerNotPresent(t *testing.T) {
	keyA := make([]byte, ed25519PublicKeyBytes)
	keyA[0] = 0xAA
	keyUnknown := make([]byte, ed25519PublicKeyBytes)
	keyUnknown[0] = 0xFF

	cfg := &Config{
		BootstrapPeers: []BootstrapPeer{
			{PeerPub: hex.EncodeToString(keyA), Addrs: []string{"relay1.example.com:60611"}},
		},
	}

	cfg.ForgetBootstrapPeer(keyUnknown)

	require.Len(t, cfg.BootstrapPeers, 1)
	require.Equal(t, hex.EncodeToString(keyA), cfg.BootstrapPeers[0].PeerPub)
}
