// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sambigeara/pollen/pkg/plnfs"
)

const (
	configFileName       = "config.yaml"
	DefaultBootstrapPort = 60611

	DefaultHTTPAddr       = ":9090"
	DefaultStaticHTTPAddr = ":8080"
	DefaultControlAddr    = ":50051"

	DefaultLogLevel = "info"

	configHeader = "# Manual edits while the daemon runs will be overwritten.\n# Use `pln serve`, `pln connect`, `pln disconnect`, `pln seed`, and `pln unseed` to manage services.\n# Use `pln set <key> [value]` and `pln unset <key>` to change daemon bind addresses or log level; restart the daemon to apply.\n\n"
)

const (
	DefaultMembershipTTL   = 4 * time.Hour
	DefaultTLSIdentityTTL  = 4 * time.Hour
	DefaultReconnectWindow = 7 * 24 * time.Hour
)

type Service struct {
	Name     string `yaml:"name"`
	Protocol string `yaml:"protocol,omitempty"`
	Port     uint32 `yaml:"port"`
}

type Connection struct {
	Service    string `yaml:"service"`
	Peer       string `yaml:"peer"`
	Protocol   string `yaml:"protocol,omitempty"`
	RemotePort uint32 `yaml:"remotePort"`
	LocalPort  uint32 `yaml:"localPort"`
}

type Placement struct {
	IdleInstanceTTL time.Duration `yaml:"idleInstanceTTL,omitempty"`
}

type Config struct {
	Name        string         `yaml:"name,omitempty"`
	HTTP        string         `yaml:"http,omitempty"`
	Properties  map[string]any `yaml:"properties,omitempty"`
	StaticHTTP  string         `yaml:"staticHTTP,omitempty"`
	ControlAddr string         `yaml:"controlAddr,omitempty"`
	LogLevel    string         `yaml:"logLevel,omitempty"`
	Connections []Connection   `yaml:"connections,omitempty"`
	Services    []Service      `yaml:"services,omitempty"`
	Placement   Placement      `yaml:"placement,omitempty"`
	Public      bool           `yaml:"public,omitempty"`
	RelayOnly   bool           `yaml:"relayOnly,omitempty"`
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
	return cfg, nil
}

func Save(pollenDir string, cfg *Config) error {
	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	return plnfs.WriteGroupWritable(filepath.Join(pollenDir, configFileName), append([]byte(configHeader), encoded...))
}

func (c *Config) AddService(name string, port uint32, protocol string) {
	if name == "" {
		name = strconv.FormatUint(uint64(port), 10)
	}
	for i, s := range c.Services {
		if s.Name == name {
			c.Services[i].Port = port
			c.Services[i].Protocol = protocol
			return
		}
	}
	c.Services = append(c.Services, Service{Name: name, Port: port, Protocol: protocol})
}

func (c *Config) RemoveService(name string) {
	c.Services = slices.DeleteFunc(c.Services, func(s Service) bool {
		return s.Name == name
	})
}

func (c *Config) AddConnection(service, peer string, remotePort, localPort uint32, protocol string) {
	for i, conn := range c.Connections {
		if conn.LocalPort == localPort && conn.Protocol == protocol {
			c.Connections[i] = Connection{
				Service:    service,
				Peer:       peer,
				RemotePort: remotePort,
				LocalPort:  localPort,
				Protocol:   protocol,
			}
			return
		}
	}
	c.Connections = append(c.Connections, Connection{
		Service:    service,
		Peer:       peer,
		RemotePort: remotePort,
		LocalPort:  localPort,
		Protocol:   protocol,
	})
}

func (c *Config) RemoveConnection(localPort uint32) {
	c.Connections = slices.DeleteFunc(c.Connections, func(conn Connection) bool {
		return conn.LocalPort == localPort
	})
}
