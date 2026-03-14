package config

import (
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForgetBootstrapPeer(t *testing.T) {
	keyA := make([]byte, ed25519.PublicKeySize)
	keyA[0] = 0xAA
	keyB := make([]byte, ed25519.PublicKeySize)
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
	keyA := make([]byte, ed25519.PublicKeySize)
	keyA[0] = 0xAA
	keyUnknown := make([]byte, ed25519.PublicKeySize)
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

func TestAddService(t *testing.T) {
	cfg := &Config{}

	cfg.AddService("http", 8080)
	require.Len(t, cfg.Services, 1)
	require.Equal(t, Service{Name: "http", Port: 8080}, cfg.Services[0])

	cfg.AddService("dns", 53)
	require.Len(t, cfg.Services, 2)
	require.Equal(t, "dns", cfg.Services[0].Name)
	require.Equal(t, "http", cfg.Services[1].Name)

	cfg.AddService("http", 9090)
	require.Len(t, cfg.Services, 2)
	require.Equal(t, uint32(9090), cfg.Services[1].Port)
}

func TestAddServiceUnnamedDifferentPorts(t *testing.T) {
	cfg := &Config{}

	cfg.AddService("", 8080)
	require.Len(t, cfg.Services, 1)

	cfg.AddService("", 9090)
	require.Len(t, cfg.Services, 2)
	require.Equal(t, uint32(8080), cfg.Services[0].Port)
	require.Equal(t, uint32(9090), cfg.Services[1].Port)

	cfg.AddService("", 8080)
	require.Len(t, cfg.Services, 2)
}

func TestRemoveService(t *testing.T) {
	cfg := &Config{Services: []Service{
		{Name: "a", Port: 1},
		{Name: "b", Port: 2},
	}}

	cfg.RemoveService("a")
	require.Len(t, cfg.Services, 1)
	require.Equal(t, "b", cfg.Services[0].Name)

	cfg.RemoveService("unknown")
	require.Len(t, cfg.Services, 1)
}

func TestAddConnection(t *testing.T) {
	cfg := &Config{}

	cfg.AddConnection("http", "deadbeef", 80, 8080)
	require.Len(t, cfg.Connections, 1)
	require.Equal(t, uint32(80), cfg.Connections[0].RemotePort)

	cfg.AddConnection("http", "deadbeef", 80, 8080)
	require.Len(t, cfg.Connections, 1)

	cfg.AddConnection("http", "deadbeef", 80, 9090)
	require.Len(t, cfg.Connections, 2)
}

func TestRemoveConnection(t *testing.T) {
	cfg := &Config{Connections: []Connection{
		{Service: "http", Peer: "aaa", LocalPort: 8080},
		{Service: "http", Peer: "bbb", LocalPort: 9090},
		{Service: "dns", Peer: "aaa", LocalPort: 53},
	}}

	cfg.RemoveConnection("http", "aaa", 0)
	require.Len(t, cfg.Connections, 2)

	cfg.RemoveConnection("", "", 9090)
	require.Len(t, cfg.Connections, 1)
	require.Equal(t, "dns", cfg.Connections[0].Service)
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()

	cfg := &Config{
		Port:         12345,
		AdvertiseIPs: []string{"1.2.3.4", "5.6.7.8"},
		Services: []Service{
			{Name: "http", Port: 8080},
		},
		Connections: []Connection{
			{Service: "http", Peer: "deadbeef", RemotePort: 80, LocalPort: 9090},
		},
	}

	require.NoError(t, Save(dir, cfg))

	raw, err := os.ReadFile(filepath.Join(dir, configFileName))
	require.NoError(t, err)
	require.Contains(t, string(raw), "# Manual edits while the daemon runs")

	loaded, err := Load(dir)
	require.NoError(t, err)
	require.Equal(t, cfg.Port, loaded.Port)
	require.Equal(t, cfg.AdvertiseIPs, loaded.AdvertiseIPs)
	require.Equal(t, cfg.Services, loaded.Services)
	require.Equal(t, cfg.Connections, loaded.Connections)
}
