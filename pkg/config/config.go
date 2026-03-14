package config

import (
	"bytes"
	"cmp"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/perm"
	"gopkg.in/yaml.v3"
)

const (
	configFileName       = "config.yaml"
	DefaultBootstrapPort = 60611

	configHeader = "# Manual edits while the daemon runs will be overwritten.\n# Use `pln serve`, `pln connect`, `pln disconnect`, `pln seed`, and `pln unseed` to manage services.\n\n"
)

type bootstrapPeer struct {
	PeerPub string   `yaml:"peerPub"`
	Addrs   []string `yaml:"addrs,omitempty"`
}

type CertTTLs struct {
	Membership      time.Duration `yaml:"membership,omitempty"`
	Delegation      time.Duration `yaml:"delegation,omitempty"`
	TLSIdentity     time.Duration `yaml:"tlsIdentity,omitempty"`
	ReconnectWindow time.Duration `yaml:"reconnectWindow,omitempty"`
}

const (
	defaultMembershipTTL   = 4 * time.Hour
	defaultDelegationTTL   = 30 * 24 * time.Hour
	defaultTLSIdentityTTL  = 4 * time.Hour
	defaultReconnectWindow = 7 * 24 * time.Hour
)

func ttlOrDefault(ttl, fallback time.Duration) time.Duration {
	if ttl == 0 {
		return fallback
	}
	return ttl
}

func (c CertTTLs) MembershipTTL() time.Duration {
	return ttlOrDefault(c.Membership, defaultMembershipTTL)
}

func (c CertTTLs) DelegationTTL() time.Duration {
	return ttlOrDefault(c.Delegation, defaultDelegationTTL)
}

func (c CertTTLs) TLSIdentityTTL() time.Duration {
	return ttlOrDefault(c.TLSIdentity, defaultTLSIdentityTTL)
}

func (c CertTTLs) ReconnectWindowDuration() time.Duration {
	return ttlOrDefault(c.ReconnectWindow, defaultReconnectWindow)
}

type service struct {
	Name string `yaml:"name"`
	Port uint32 `yaml:"port"`
}

type connection struct {
	Service    string `yaml:"service"`
	Peer       string `yaml:"peer"`
	RemotePort uint32 `yaml:"remotePort"`
	LocalPort  uint32 `yaml:"localPort"`
}

type Config struct {
	AdvertiseIPs   []string        `yaml:"advertiseIPs,omitempty"`
	BootstrapPeers []bootstrapPeer `yaml:"bootstrapPeers,omitempty"`
	Services       []service       `yaml:"services,omitempty"`
	Connections    []connection    `yaml:"connections,omitempty"`
	CertTTLs       CertTTLs        `yaml:"certTTLs,omitempty"` //nolint:tagliatelle
	Port           uint32          `yaml:"port,omitempty"`
	Public         bool            `yaml:"public,omitempty"`
}

func Load(pollenDir string) (*Config, error) {
	path := filepath.Join(pollenDir, configFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Config{}, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &Config{}
	if len(bytes.TrimSpace(raw)) == 0 {
		return cfg, nil
	}

	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if err := validateCertTTLs(cfg.CertTTLs); err != nil {
		return nil, err
	}

	canonical, err := canonicalizeBootstrapPeers(cfg.BootstrapPeers)
	if err != nil {
		return nil, err
	}
	cfg.BootstrapPeers = canonical
	return cfg, nil
}

func Save(pollenDir string, cfg *Config) error {
	if cfg == nil {
		cfg = &Config{}
	}

	if err := validateCertTTLs(cfg.CertTTLs); err != nil {
		return err
	}

	canonical, err := canonicalizeBootstrapPeers(cfg.BootstrapPeers)
	if err != nil {
		return err
	}
	cfg.BootstrapPeers = canonical

	if err := perm.EnsureDir(pollenDir); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}

	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	return perm.WriteGroupWritable(filepath.Join(pollenDir, configFileName), append([]byte(configHeader), encoded...))
}

func validateCertTTLs(ttls CertTTLs) error {
	if ttls.Membership < 0 {
		return errors.New("certTTLs.membership must be >= 0")
	}
	if ttls.Delegation < 0 {
		return errors.New("certTTLs.delegation must be >= 0")
	}
	if ttls.TLSIdentity < 0 {
		return errors.New("certTTLs.tlsIdentity must be >= 0")
	}
	if ttls.ReconnectWindow < 0 {
		return errors.New("certTTLs.reconnectWindow must be >= 0")
	}
	return nil
}

func (c *Config) RememberBootstrapPeer(peer *admissionv1.BootstrapPeer) error {
	if peer == nil {
		return errors.New("missing bootstrap peer")
	}

	pub := peer.GetPeerPub()
	if len(pub) != ed25519.PublicKeySize {
		return errors.New("invalid bootstrap peer key length")
	}

	c.BootstrapPeers = append(c.BootstrapPeers, bootstrapPeer{
		PeerPub: hex.EncodeToString(pub),
		Addrs:   append([]string(nil), peer.GetAddrs()...),
	})

	canonical, err := canonicalizeBootstrapPeers(c.BootstrapPeers)
	if err != nil {
		return err
	}
	c.BootstrapPeers = canonical

	return nil
}

func (c *Config) ForgetBootstrapPeer(pubKey []byte) {
	hexKey := hex.EncodeToString(pubKey)
	c.BootstrapPeers = slices.DeleteFunc(c.BootstrapPeers, func(bp bootstrapPeer) bool {
		return bp.PeerPub == hexKey
	})
}

func (c *Config) BootstrapProtoPeers() ([]*admissionv1.BootstrapPeer, error) {
	peers := make([]*admissionv1.BootstrapPeer, 0, len(c.BootstrapPeers))
	for idx, peer := range c.BootstrapPeers {
		pub, err := decodePubHex(peer.PeerPub)
		if err != nil {
			return nil, fmt.Errorf("parse bootstrap peer[%d] public key: %w", idx, err)
		}

		addrs, err := normalizeAddrs(peer.Addrs)
		if err != nil {
			return nil, fmt.Errorf("parse bootstrap peer[%d] addresses: %w", idx, err)
		}

		peers = append(peers, &admissionv1.BootstrapPeer{
			PeerPub: pub,
			Addrs:   addrs,
		})
	}

	return peers, nil
}

func canonicalizeBootstrapPeers(peers []bootstrapPeer) ([]bootstrapPeer, error) {
	byPeer := make(map[string]map[string]struct{})

	for _, peer := range peers {
		if strings.TrimSpace(peer.PeerPub) == "" {
			continue
		}

		pub, err := decodePubHex(peer.PeerPub)
		if err != nil {
			return nil, err
		}
		peerHex := hex.EncodeToString(pub)

		if _, ok := byPeer[peerHex]; !ok {
			byPeer[peerHex] = make(map[string]struct{})
		}

		addrs, err := normalizeAddrs(peer.Addrs)
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			byPeer[peerHex][addr] = struct{}{}
		}
	}

	peerHexes := slices.Sorted(maps.Keys(byPeer))

	out := make([]bootstrapPeer, 0, len(peerHexes))
	for _, peerHex := range peerHexes {
		addrs := slices.Sorted(maps.Keys(byPeer[peerHex]))
		if len(addrs) == 0 {
			return nil, fmt.Errorf("bootstrap peer %s has no addresses", peerHex)
		}
		out = append(out, bootstrapPeer{
			PeerPub: peerHex,
			Addrs:   addrs,
		})
	}

	return out, nil
}

func decodePubHex(v string) ([]byte, error) {
	b, err := hex.DecodeString(strings.TrimSpace(v))
	if err != nil {
		return nil, fmt.Errorf("invalid public key encoding: %w", err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid public key length: expected %d bytes", ed25519.PublicKeySize)
	}

	return b, nil
}

func normalizeAddrs(specs []string) ([]string, error) {
	out := make([]string, 0, len(specs))
	for _, spec := range specs {
		addr, err := NormalizeRelayAddr(spec)
		if err != nil {
			return nil, err
		}
		if !slices.Contains(out, addr) {
			out = append(out, addr)
		}
	}

	if len(out) == 0 {
		return nil, errors.New("at least one relay address is required")
	}

	return out, nil
}

func NormalizeRelayAddr(spec string) (string, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", errors.New("relay address cannot be empty")
	}

	if _, _, err := net.SplitHostPort(spec); err == nil {
		return spec, nil
	}

	return net.JoinHostPort(spec, strconv.Itoa(DefaultBootstrapPort)), nil
}

func (c *Config) AddService(name string, port uint32) {
	if name == "" {
		for i, s := range c.Services {
			if s.Name == "" && s.Port == port {
				c.Services[i] = service{Port: port}
				return
			}
		}
	} else {
		for i, s := range c.Services {
			if s.Name == name {
				c.Services[i].Port = port
				return
			}
		}
	}
	c.Services = append(c.Services, service{Name: name, Port: port})
	c.canonicalizeServices()
}

func (c *Config) RemoveService(name string) {
	c.Services = slices.DeleteFunc(c.Services, func(s service) bool {
		return s.Name == name
	})
}

func (c *Config) RemoveServiceByPort(port uint32) {
	c.Services = slices.DeleteFunc(c.Services, func(s service) bool {
		return s.Port == port
	})
}

func (c *Config) AddConnection(service, peer string, remotePort, localPort uint32) {
	for _, conn := range c.Connections {
		if conn.Service == service && conn.Peer == peer && conn.LocalPort == localPort {
			return
		}
	}
	c.Connections = append(c.Connections, connection{
		Service:    service,
		Peer:       peer,
		RemotePort: remotePort,
		LocalPort:  localPort,
	})
	c.canonicalizeConnections()
}

// RemoveConnection removes connections matching the given fields.
// Zero-value fields act as wildcards. All-zero is a no-op.
func (c *Config) RemoveConnection(service, peer string, localPort uint32) {
	if service == "" && peer == "" && localPort == 0 {
		return
	}
	c.Connections = slices.DeleteFunc(c.Connections, func(conn connection) bool {
		if service != "" && conn.Service != service {
			return false
		}
		if peer != "" && conn.Peer != peer {
			return false
		}
		if localPort != 0 && conn.LocalPort != localPort {
			return false
		}
		return true
	})
}

func (c *Config) canonicalizeServices() {
	slices.SortFunc(c.Services, func(a, b service) int {
		return cmp.Compare(a.Name, b.Name)
	})
}

func (c *Config) canonicalizeConnections() {
	slices.SortFunc(c.Connections, func(a, b connection) int {
		if v := cmp.Compare(a.Service, b.Service); v != 0 {
			return v
		}
		if v := cmp.Compare(a.Peer, b.Peer); v != 0 {
			return v
		}
		return cmp.Compare(a.LocalPort, b.LocalPort)
	})
}
