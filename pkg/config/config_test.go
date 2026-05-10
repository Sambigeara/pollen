// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

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
			Public:      true,
		}

		require.NoError(t, Save(dir, cfg))

		loaded, err := Load(dir)
		require.NoError(t, err)
		require.Equal(t, cfg, loaded)
	})
}

func TestAddService(t *testing.T) {
	t.Run("appends new service", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddService("http", 8080, "")

		require.Equal(t, []Service{{Name: "http", Port: 8080}}, cfg.Services)
	})

	t.Run("updates port for existing name", func(t *testing.T) {
		cfg := &Config{Services: []Service{{Name: "http", Port: 8080}}}
		cfg.AddService("http", 9090, "")

		require.Len(t, cfg.Services, 1)
		require.Equal(t, uint32(9090), cfg.Services[0].Port)
	})

	t.Run("preserves order across names", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddService("http", 8080, "")
		cfg.AddService("dns", 53, "")

		require.Equal(t, "http", cfg.Services[0].Name)
		require.Equal(t, "dns", cfg.Services[1].Name)
	})

	t.Run("empty name defaults to port string", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddService("", 8080, "")

		require.Equal(t, "8080", cfg.Services[0].Name)
	})

	t.Run("empty name deduplicates by generated name", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddService("", 8080, "")
		cfg.AddService("", 8080, "")

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
		cfg.AddConnection("http", "deadbeef", 80, 8080, "")

		require.Equal(t, []Connection{
			{Service: "http", Peer: "deadbeef", RemotePort: 80, LocalPort: 8080},
		}, cfg.Connections)
	})

	t.Run("updates existing by local port and protocol", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddConnection("http", "aaa", 80, 8080, "")
		cfg.AddConnection("dns", "bbb", 53, 8080, "")

		require.Len(t, cfg.Connections, 1)
		require.Equal(t, Connection{Service: "dns", Peer: "bbb", RemotePort: 53, LocalPort: 8080}, cfg.Connections[0])
	})

	t.Run("different local ports create separate entries", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddConnection("http", "aaa", 80, 8080, "")
		cfg.AddConnection("http", "aaa", 80, 9090, "")

		require.Len(t, cfg.Connections, 2)
	})

	t.Run("same port different protocol creates separate entries", func(t *testing.T) {
		cfg := &Config{}
		cfg.AddConnection("dns", "aaa", 53, 5353, "")
		cfg.AddConnection("dns-udp", "aaa", 53, 5353, "udp")

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
