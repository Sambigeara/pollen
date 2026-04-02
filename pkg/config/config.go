package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"gopkg.in/yaml.v3"

	"github.com/sambigeara/pollen/pkg/plnfs"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	configFileName       = "config.yaml"
	DefaultBootstrapPort = 60611

	configHeader = "# Manual edits while the daemon runs will be overwritten.\n# Use `pln serve`, `pln connect`, `pln disconnect`, `pln seed`, and `pln unseed` to manage services.\n\n"
)

const (
	DefaultMembershipTTL   = 4 * time.Hour
	DefaultTLSIdentityTTL  = 4 * time.Hour
	DefaultReconnectWindow = 7 * 24 * time.Hour
	DefaultDelegationTTL   = 30 * 24 * time.Hour
)

type Service struct {
	Name string `yaml:"name"`
	Port uint32 `yaml:"port"`
}

type Connection struct {
	Service    string `yaml:"service"`
	Peer       string `yaml:"peer"`
	RemotePort uint32 `yaml:"remotePort"`
	LocalPort  uint32 `yaml:"localPort"`
}

type Config struct {
	BootstrapPeers map[string][]string `yaml:"bootstrapPeers,omitempty"`
	Connections    []Connection        `yaml:"connections,omitempty"`
	Services       []Service           `yaml:"services,omitempty"`
	Public         bool                `yaml:"public,omitempty"`
}

func Load(pollenDir string) (*Config, error) {
	raw, err := os.ReadFile(filepath.Join(pollenDir, configFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Config{}, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	// basic validation...
	for peerHex, addrs := range cfg.BootstrapPeers {
		if _, err := types.PeerKeyFromString(peerHex); err != nil {
			return nil, fmt.Errorf("bootstrap peer %s: %w", peerHex, err)
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("bootstrap peer %s: at least one address is required", peerHex)
		}
	}

	return cfg, nil
}

func Save(pollenDir string, cfg *Config) error {
	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	return plnfs.WriteGroupWritable(filepath.Join(pollenDir, configFileName), append([]byte(configHeader), encoded...))
}

func (c *Config) RememberBootstrapPeer(peer *admissionv1.BootstrapPeer) {
	if c.BootstrapPeers == nil {
		c.BootstrapPeers = make(map[string][]string)
	}
	pk := types.PeerKeyFromBytes(peer.GetPeerPub())
	c.BootstrapPeers[pk.String()] = slices.Clone(peer.GetAddrs())
}

func (c *Config) ForgetBootstrapPeer(pubKey []byte) {
	delete(c.BootstrapPeers, hex.EncodeToString(pubKey))
}

func (c *Config) AddService(name string, port uint32) {
	if name == "" {
		name = strconv.FormatUint(uint64(port), 10)
	}
	for i, s := range c.Services {
		if s.Name == name {
			c.Services[i].Port = port
			return
		}
	}
	c.Services = append(c.Services, Service{Name: name, Port: port})
}

func (c *Config) RemoveService(name string) {
	c.Services = slices.DeleteFunc(c.Services, func(s Service) bool {
		return s.Name == name
	})
}

func (c *Config) AddConnection(service, peer string, remotePort, localPort uint32) {
	for i, conn := range c.Connections {
		if conn.LocalPort == localPort {
			c.Connections[i] = Connection{
				Service:    service,
				Peer:       peer,
				RemotePort: remotePort,
				LocalPort:  localPort,
			}
			return
		}
	}
	c.Connections = append(c.Connections, Connection{
		Service:    service,
		Peer:       peer,
		RemotePort: remotePort,
		LocalPort:  localPort,
	})
}

func (c *Config) RemoveConnection(localPort uint32) {
	c.Connections = slices.DeleteFunc(c.Connections, func(conn Connection) bool {
		return conn.LocalPort == localPort
	})
}
