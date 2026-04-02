package config

import (
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/stretchr/testify/require"
)

func testKey(b byte) []byte {
	k := make([]byte, ed25519.PublicKeySize)
	k[0] = b
	return k
}

func TestLoad(t *testing.T) {
	t.Run("missing file returns empty config", func(t *testing.T) {
		cfg, err := Load(t.TempDir())
		require.NoError(t, err)
		require.Equal(t, &Config{}, cfg)
	})

	t.Run("valid config", func(t *testing.T) {
		dir := t.TempDir()
		yaml := "services:\n  - name: http\n    port: 8080\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, configFileName), []byte(yaml), 0o600))

		cfg, err := Load(dir)
		require.NoError(t, err)
		require.Equal(t, []Service{{Name: "http", Port: 8080}}, cfg.Services)
	})

	t.Run("invalid yaml", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, configFileName), []byte("services: [[["), 0o600))

		_, err := Load(dir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unmarshal config")
	})

	t.Run("invalid bootstrap peer key", func(t *testing.T) {
		dir := t.TempDir()
		yaml := "bootstrapPeers:\n  not-hex:\n    - relay.example.com:60611\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, configFileName), []byte(yaml), 0o600))

		_, err := Load(dir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "bootstrap peer not-hex")
	})

	t.Run("bootstrap peer with short key", func(t *testing.T) {
		dir := t.TempDir()
		yaml := "bootstrapPeers:\n  aabb:\n    - relay.example.com:60611\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, configFileName), []byte(yaml), 0o600))

		_, err := Load(dir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "bootstrap peer aabb")
	})

	t.Run("bootstrap peer with no addresses", func(t *testing.T) {
		dir := t.TempDir()
		hexKey := hex.EncodeToString(testKey(0xAA))
		yaml := "bootstrapPeers:\n  " + hexKey + ": []\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, configFileName), []byte(yaml), 0o600))

		_, err := Load(dir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one address is required")
	})
}

func TestSave(t *testing.T) {
	t.Run("includes header", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, Save(dir, &Config{}))

		raw, err := os.ReadFile(filepath.Join(dir, configFileName))
		require.NoError(t, err)
		require.Contains(t, string(raw), "# Manual edits while the daemon runs")
	})

	t.Run("round trip", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &Config{
			Services:    []Service{{Name: "http", Port: 8080}},
			Connections: []Connection{{Service: "http", Peer: "deadbeef", RemotePort: 80, LocalPort: 9090}},
			BootstrapPeers: map[string][]string{
				hex.EncodeToString(testKey(0xAA)): {"relay.example.com:60611"},
			},
			Public: true,
		}

		require.NoError(t, Save(dir, cfg))

		loaded, err := Load(dir)
		require.NoError(t, err)
		require.Equal(t, cfg, loaded)
	})
}

func TestRememberBootstrapPeer(t *testing.T) {
	t.Run("initializes nil map", func(t *testing.T) {
		cfg := &Config{}
		cfg.RememberBootstrapPeer(&admissionv1.BootstrapPeer{
			PeerPub: testKey(0xAA),
			Addrs:   []string{"relay.example.com:60611"},
		})

		hexKey := hex.EncodeToString(testKey(0xAA))
		require.Len(t, cfg.BootstrapPeers, 1)
		require.Equal(t, []string{"relay.example.com:60611"}, cfg.BootstrapPeers[hexKey])
	})

	t.Run("overwrites existing peer", func(t *testing.T) {
		hexKey := hex.EncodeToString(testKey(0xAA))
		cfg := &Config{
			BootstrapPeers: map[string][]string{
				hexKey: {"old.example.com:60611"},
			},
		}

		cfg.RememberBootstrapPeer(&admissionv1.BootstrapPeer{
			PeerPub: testKey(0xAA),
			Addrs:   []string{"new.example.com:60611"},
		})

		require.Len(t, cfg.BootstrapPeers, 1)
		require.Equal(t, []string{"new.example.com:60611"}, cfg.BootstrapPeers[hexKey])
	})
}

func TestForgetBootstrapPeer(t *testing.T) {
	t.Run("removes existing peer", func(t *testing.T) {
		keyA := testKey(0xAA)
		keyB := testKey(0xBB)
		cfg := &Config{
			BootstrapPeers: map[string][]string{
				hex.EncodeToString(keyA): {"relay1.example.com:60611"},
				hex.EncodeToString(keyB): {"relay2.example.com:60611"},
			},
		}

		cfg.ForgetBootstrapPeer(keyA)

		require.Len(t, cfg.BootstrapPeers, 1)
		require.Contains(t, cfg.BootstrapPeers, hex.EncodeToString(keyB))
	})

	t.Run("no-op for unknown peer", func(t *testing.T) {
		keyA := testKey(0xAA)
		cfg := &Config{
			BootstrapPeers: map[string][]string{
				hex.EncodeToString(keyA): {"relay.example.com:60611"},
			},
		}

		cfg.ForgetBootstrapPeer(testKey(0xFF))

		require.Len(t, cfg.BootstrapPeers, 1)
	})
}

func TestAddService(t *testing.T) {
	t.Run("appends new service", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddService("http", 8080)

		require.Equal(t, []Service{{Name: "http", Port: 8080}}, cfg.Services)
	})

	t.Run("updates port for existing name", func(t *testing.T) {
		cfg := &Config{Services: []Service{{Name: "http", Port: 8080}}}
		cfg.AddService("http", 9090)

		require.Len(t, cfg.Services, 1)
		require.Equal(t, uint32(9090), cfg.Services[0].Port)
	})

	t.Run("preserves order across names", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddService("http", 8080)
		cfg.AddService("dns", 53)

		require.Equal(t, "http", cfg.Services[0].Name)
		require.Equal(t, "dns", cfg.Services[1].Name)
	})

	t.Run("empty name defaults to port string", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddService("", 8080)

		require.Equal(t, "8080", cfg.Services[0].Name)
	})

	t.Run("empty name deduplicates by generated name", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddService("", 8080)
		cfg.AddService("", 8080)

		require.Len(t, cfg.Services, 1)
	})
}

func TestRemoveService(t *testing.T) {
	t.Run("removes matching service", func(t *testing.T) {
		cfg := &Config{Services: []Service{
			{Name: "http", Port: 8080},
			{Name: "dns", Port: 53},
		}}

		cfg.RemoveService("http")

		require.Equal(t, []Service{{Name: "dns", Port: 53}}, cfg.Services)
	})

	t.Run("no-op for unknown name", func(t *testing.T) {
		cfg := &Config{Services: []Service{{Name: "http", Port: 8080}}}
		cfg.RemoveService("unknown")

		require.Len(t, cfg.Services, 1)
	})
}

func TestAddConnection(t *testing.T) {
	t.Run("appends new connection", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddConnection("http", "deadbeef", 80, 8080)

		require.Equal(t, []Connection{
			{Service: "http", Peer: "deadbeef", RemotePort: 80, LocalPort: 8080},
		}, cfg.Connections)
	})

	t.Run("updates existing by local port", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddConnection("http", "aaa", 80, 8080)
		cfg.AddConnection("dns", "bbb", 53, 8080)

		require.Len(t, cfg.Connections, 1)
		require.Equal(t, Connection{Service: "dns", Peer: "bbb", RemotePort: 53, LocalPort: 8080}, cfg.Connections[0])
	})

	t.Run("different local ports create separate entries", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddConnection("http", "aaa", 80, 8080)
		cfg.AddConnection("http", "aaa", 80, 9090)

		require.Len(t, cfg.Connections, 2)
	})
}

func TestRemoveConnection(t *testing.T) {
	t.Run("removes by local port", func(t *testing.T) {
		cfg := &Config{Connections: []Connection{
			{Service: "http", Peer: "aaa", LocalPort: 8080},
			{Service: "dns", Peer: "bbb", LocalPort: 53},
		}}

		cfg.RemoveConnection(8080)

		require.Equal(t, []Connection{{Service: "dns", Peer: "bbb", LocalPort: 53}}, cfg.Connections)
	})

	t.Run("no-op for unknown port", func(t *testing.T) {
		cfg := &Config{Connections: []Connection{{Service: "http", Peer: "aaa", LocalPort: 8080}}}
		cfg.RemoveConnection(9999)

		require.Len(t, cfg.Connections, 1)
	})
}
