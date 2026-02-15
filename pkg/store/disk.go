package store

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"

	"github.com/sambigeara/pollen/pkg/types"
	"gopkg.in/yaml.v3"
)

const (
	configFileName = "config.yaml"
	lockFileName   = ".config.lock"

	keyDirPerm    = 0o700
	filePerm      = 0o600
	stateFilePerm = 0o644
)

type diskState struct {
	Local       diskLocal        `yaml:"local"`
	Peers       []diskPeer       `yaml:"peers,omitempty"`
	Services    []diskService    `yaml:"services,omitempty"`
	Connections []diskConnection `yaml:"connections,omitempty"`
}

type diskLocal struct {
	IdentityPublic string `yaml:"identityPublic"`
}

type diskPeer struct {
	IdentityPublic string   `yaml:"identityPublic"`
	Addresses      []string `yaml:"addresses,omitempty"`
	Port           uint32   `yaml:"port,omitempty"`
}

type diskService struct {
	Name     string `yaml:"name"`
	Provider string `yaml:"provider"`
	Port     uint32 `yaml:"port"`
}

type diskConnection struct {
	Peer       string `yaml:"peer"`
	RemotePort uint32 `yaml:"remotePort"`
	LocalPort  uint32 `yaml:"localPort"`
}

type disk struct {
	lockFile   *os.File
	configPath string
	mu         sync.Mutex
}

func openDisk(pollenDir string) (*disk, error) {
	if err := os.MkdirAll(pollenDir, keyDirPerm); err != nil {
		return nil, fmt.Errorf("create pollen dir: %w", err)
	}

	lockPath := filepath.Join(pollenDir, lockFileName)
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, filePerm)
	if err != nil {
		return nil, fmt.Errorf("open config lock: %w", err)
	}

	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("lock config: %w", err)
	}

	configPath := filepath.Join(pollenDir, configFileName)
	if _, err := os.Stat(configPath); err != nil {
		if os.IsNotExist(err) {
			if err := os.WriteFile(configPath, []byte("local:\n  noisePublic: \"\"\n  identityPublic: \"\"\n"), stateFilePerm); err != nil {
				_ = syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
				_ = lf.Close()
				return nil, fmt.Errorf("create config: %w", err)
			}
		} else {
			_ = syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
			_ = lf.Close()
			return nil, fmt.Errorf("stat config: %w", err)
		}
	}

	return &disk{configPath: configPath, lockFile: lf}, nil
}

func (d *disk) close() error {
	if d == nil || d.lockFile == nil {
		return nil
	}
	if err := syscall.Flock(int(d.lockFile.Fd()), syscall.LOCK_UN); err != nil {
		_ = d.lockFile.Close()
		return err
	}
	return d.lockFile.Close()
}

func (d *disk) load() (diskState, error) {
	b, err := os.ReadFile(d.configPath)
	if err != nil {
		return diskState{}, fmt.Errorf("read config: %w", err)
	}

	st := diskState{}
	if len(bytes.TrimSpace(b)) == 0 {
		return st, nil
	}

	if err := yaml.Unmarshal(b, &st); err != nil {
		return diskState{}, fmt.Errorf("unmarshal config: %w", err)
	}

	return st, nil
}

func (d *disk) save(st diskState) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	b, err := yaml.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	tmp := d.configPath + ".tmp"
	if err := os.WriteFile(tmp, b, stateFilePerm); err != nil {
		return fmt.Errorf("write temp config: %w", err)
	}

	f, err := os.Open(tmp)
	if err != nil {
		return fmt.Errorf("open temp config: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync temp config: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp config: %w", err)
	}

	if err := os.Rename(tmp, d.configPath); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}

	return nil
}

func encodeHex(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(b)
}

func decodeHex(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func toDiskServices(nodes map[types.PeerKey]nodeRecord) []diskService {
	services := make([]diskService, 0)
	for peerID, rec := range nodes {
		for _, svc := range rec.Services {
			if svc == nil {
				continue
			}
			services = append(services, diskService{
				Name:     svc.GetName(),
				Provider: peerID.String(),
				Port:     svc.GetPort(),
			})
		}
	}

	sort.Slice(services, func(i, j int) bool {
		a := services[i]
		b := services[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		return a.Port < b.Port
	})

	return services
}
