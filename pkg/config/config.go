package config

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"gopkg.in/yaml.v3"
)

const (
	configFileName        = "config.yaml"
	DefaultBootstrapPort  = 60611
	directoryPerm         = 0o700
	configFilePerm        = 0o600
	ed25519PublicKeyBytes = 32

	DefaultMembershipDays  = 365
	DefaultAdminDays       = 1825 // 5 * 365
	DefaultTLSIdentityDays = 3650 // 10 * 365
	hoursPerDay            = 24
)

type BootstrapPeer struct {
	PeerPub string   `yaml:"peerPub"`
	Addrs   []string `yaml:"addrs,omitempty"`
}

type CertTTLs struct { //nolint:tagliatelle
	MembershipDays  int `yaml:"membershipDays,omitempty"`
	AdminDays       int `yaml:"adminDays,omitempty"`
	TLSIdentityDays int `yaml:"tlsIdentityDays,omitempty"`
}

type Config struct {
	CertTTLs       *CertTTLs       `yaml:"certTTLs,omitempty"` //nolint:tagliatelle
	BootstrapPeers []BootstrapPeer `yaml:"bootstrapPeers,omitempty"`
}

func (c *Config) MembershipTTL() time.Duration {
	if c != nil && c.CertTTLs != nil && c.CertTTLs.MembershipDays > 0 {
		return time.Duration(c.CertTTLs.MembershipDays) * hoursPerDay * time.Hour
	}
	return DefaultMembershipDays * hoursPerDay * time.Hour
}

func (c *Config) AdminCertTTL() time.Duration {
	if c != nil && c.CertTTLs != nil && c.CertTTLs.AdminDays > 0 {
		return time.Duration(c.CertTTLs.AdminDays) * hoursPerDay * time.Hour
	}
	return DefaultAdminDays * hoursPerDay * time.Hour
}

func (c *Config) TLSIdentityTTL() time.Duration {
	if c != nil && c.CertTTLs != nil && c.CertTTLs.TLSIdentityDays > 0 {
		return time.Duration(c.CertTTLs.TLSIdentityDays) * hoursPerDay * time.Hour
	}
	return DefaultTLSIdentityDays * hoursPerDay * time.Hour
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

	canonical, err := canonicalizeBootstrapPeers(cfg.BootstrapPeers)
	if err != nil {
		return err
	}
	cfg.BootstrapPeers = canonical

	if err := os.MkdirAll(pollenDir, directoryPerm); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}

	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	path := filepath.Join(pollenDir, configFileName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, encoded, configFilePerm); err != nil {
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}

	return nil
}

func (c *Config) RememberBootstrapPeer(peer *admissionv1.BootstrapPeer) error {
	if c == nil {
		return errors.New("missing config")
	}
	if peer == nil {
		return errors.New("missing bootstrap peer")
	}

	pub := peer.GetPeerPub()
	if len(pub) != ed25519PublicKeyBytes {
		return errors.New("invalid bootstrap peer key length")
	}

	c.BootstrapPeers = append(c.BootstrapPeers, BootstrapPeer{
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

func (c *Config) BootstrapProtoPeers() ([]*admissionv1.BootstrapPeer, error) {
	if c == nil {
		return nil, nil
	}

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

func canonicalizeBootstrapPeers(peers []BootstrapPeer) ([]BootstrapPeer, error) {
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

	peerHexes := make([]string, 0, len(byPeer))
	for peerHex := range byPeer {
		peerHexes = append(peerHexes, peerHex)
	}
	sort.Strings(peerHexes)

	out := make([]BootstrapPeer, 0, len(peerHexes))
	for _, peerHex := range peerHexes {
		addrsSet := byPeer[peerHex]
		if len(addrsSet) == 0 {
			return nil, fmt.Errorf("bootstrap peer %s has no addresses", peerHex)
		}

		addrs := make([]string, 0, len(addrsSet))
		for addr := range addrsSet {
			addrs = append(addrs, strings.TrimSpace(addr))
		}
		sort.Strings(addrs)

		out = append(out, BootstrapPeer{
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
	if len(b) != ed25519PublicKeyBytes {
		return nil, fmt.Errorf("invalid public key length: expected %d bytes", ed25519PublicKeyBytes)
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
