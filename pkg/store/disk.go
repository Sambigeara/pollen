package store

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
	"gopkg.in/yaml.v3"
)

const (
	stateFileName = "state.yaml"
	lockFileName  = ".state.lock"

	keyDirPerm    = 0o700
	filePerm      = 0o600
	stateFilePerm = 0o644
)

type diskState struct {
	Local       diskLocal        `yaml:"local"`
	Peers       []diskPeer       `yaml:"peers,omitempty"`
	Services    []diskService    `yaml:"services,omitempty"`
	Connections []diskConnection `yaml:"connections,omitempty"`
	Revocations []diskRevocation `yaml:"revocations,omitempty"`
}

type diskRevocation struct {
	SubjectPub string `yaml:"subjectPub"`
	Data       string `yaml:"data"`
}

type diskLocal struct {
	IdentityPublic string `yaml:"identityPublic"`
}

type diskPeer struct {
	IdentityPublic string   `yaml:"identityPublic"`
	LastAddr       string   `yaml:"lastAddr,omitempty"`
	Addresses      []string `yaml:"addresses,omitempty"`
	Port           uint32   `yaml:"port,omitempty"`
	ExternalPort   uint32   `yaml:"externalPort,omitempty"`
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
	lockFile  *os.File
	statePath string
	mu        sync.Mutex
}

func openDisk(pollenDir string) (*disk, error) {
	if err := os.MkdirAll(pollenDir, keyDirPerm); err != nil {
		return nil, fmt.Errorf("create pollen dir: %w", err)
	}

	lockPath := filepath.Join(pollenDir, lockFileName)
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, filePerm)
	if err != nil {
		return nil, fmt.Errorf("open state lock: %w", err)
	}

	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("lock state: %w", err)
	}

	statePath := filepath.Join(pollenDir, stateFileName)
	if _, err := os.Stat(statePath); errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(statePath, []byte("local:\n  identityPublic: \"\"\n"), stateFilePerm); err != nil {
			_ = syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
			_ = lf.Close()
			return nil, fmt.Errorf("create state: %w", err)
		}
	} else if err != nil {
		_ = syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
		_ = lf.Close()
		return nil, fmt.Errorf("stat state: %w", err)
	}

	return &disk{statePath: statePath, lockFile: lf}, nil
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
	b, err := os.ReadFile(d.statePath)
	if err != nil {
		return diskState{}, fmt.Errorf("read state: %w", err)
	}

	var st diskState
	if len(bytes.TrimSpace(b)) == 0 {
		return st, nil
	}
	if err := yaml.Unmarshal(b, &st); err != nil {
		return st, fmt.Errorf("unmarshal state: %w", err)
	}
	return st, nil
}

func (d *disk) save(st diskState) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	b, err := yaml.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	tmp := d.statePath + ".tmp"
	if err := os.WriteFile(tmp, b, stateFilePerm); err != nil {
		return fmt.Errorf("write temp state: %w", err)
	}

	f, err := os.Open(tmp)
	if err != nil {
		return fmt.Errorf("open temp state: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync temp state: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp state: %w", err)
	}

	if err := os.Rename(tmp, d.statePath); err != nil {
		return fmt.Errorf("replace state: %w", err)
	}

	return nil
}

func marshalDiskRevocations(revocations map[types.PeerKey]*admissionv1.SignedRevocation) []diskRevocation {
	if len(revocations) == 0 {
		return nil
	}

	revs := make([]diskRevocation, 0, len(revocations))
	for subjectKey, rev := range revocations {
		raw, err := rev.MarshalVT()
		if err != nil {
			slog.Warn("failed to marshal revocation for disk", "subject", subjectKey.String(), "err", err)
			continue
		}
		revs = append(revs, diskRevocation{
			SubjectPub: subjectKey.String(),
			Data:       base64.StdEncoding.EncodeToString(raw),
		})
	}

	slices.SortFunc(revs, func(a, b diskRevocation) int {
		return cmp.Compare(a.SubjectPub, b.SubjectPub)
	})

	return revs
}

func unmarshalDiskRevocations(diskRevs []diskRevocation, trustBundle *admissionv1.TrustBundle) map[types.PeerKey]*admissionv1.SignedRevocation {
	revocations := make(map[types.PeerKey]*admissionv1.SignedRevocation, len(diskRevs))
	for _, dr := range diskRevs {
		subjectKey, err := types.PeerKeyFromString(dr.SubjectPub)
		if err != nil {
			slog.Warn("failed to parse revocation subject key from disk", "subject", dr.SubjectPub, "err", err)
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(dr.Data)
		if err != nil {
			slog.Warn("failed to decode revocation data from disk", "subject", dr.SubjectPub, "err", err)
			continue
		}
		rev := &admissionv1.SignedRevocation{}
		if err := rev.UnmarshalVT(raw); err != nil {
			slog.Warn("failed to unmarshal revocation from disk", "subject", dr.SubjectPub, "err", err)
			continue
		}
		if trustBundle != nil {
			if err := auth.VerifyRevocation(rev, trustBundle); err != nil {
				slog.Warn("failed to verify revocation from disk", "subject", dr.SubjectPub, "err", err)
				continue
			}
		}
		revocations[subjectKey] = rev
	}
	return revocations
}

func toDiskServices(nodes map[types.PeerKey]nodeRecord) []diskService {
	var services []diskService
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

	slices.SortFunc(services, func(a, b diskService) int {
		if c := cmp.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Provider, b.Provider); c != 0 {
			return c
		}
		return cmp.Compare(a.Port, b.Port)
	})

	return services
}
