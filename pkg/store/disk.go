package store

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/perm"
	"github.com/sambigeara/pollen/pkg/types"
	"gopkg.in/yaml.v3"
)

const (
	stateFileName     = "state.pb"
	stateYAMLFileName = "state.yaml"
	lockFileName      = ".state.lock"

	filePerm = 0o600
)

type disk struct {
	lockFile  *os.File
	statePath string
	mu        sync.Mutex
}

func openDisk(pollenDir string) (*disk, error) {
	if err := os.MkdirAll(pollenDir, 0o700); err != nil { //nolint:mnd
		return nil, fmt.Errorf("create pollen dir: %w", err)
	}

	lockPath := filepath.Join(pollenDir, lockFileName)
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, filePerm)
	if err != nil {
		return nil, fmt.Errorf("open state lock: %w", err)
	}
	if err := perm.SetPrivate(lockPath); err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("set lock permissions: %w", err)
	}

	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("lock state: %w", err)
	}

	statePath := filepath.Join(pollenDir, stateFileName)

	if err := ensureStateFile(pollenDir, statePath); err != nil {
		_ = syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
		_ = lf.Close()
		return nil, err
	}

	return &disk{statePath: statePath, lockFile: lf}, nil
}

func ensureStateFile(pollenDir, statePath string) error {
	if err := ensureProtoState(pollenDir, statePath); err != nil {
		return err
	}
	if err := migrateConsumedInvitesJSON(pollenDir, statePath); err != nil {
		return fmt.Errorf("migrate consumed invites: %w", err)
	}
	return nil
}

func ensureProtoState(pollenDir, statePath string) error {
	_, err := os.Stat(statePath)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat state: %w", err)
	}

	yamlPath := filepath.Join(pollenDir, stateYAMLFileName)
	migrated, mErr := migrateYAMLToProto(yamlPath, statePath)
	if mErr != nil {
		return fmt.Errorf("migrate state: %w", mErr)
	}
	if migrated {
		return nil
	}

	if err := perm.WriteGroupReadable(statePath, nil); err != nil {
		return fmt.Errorf("create state: %w", err)
	}
	return nil
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

func (d *disk) load() (*statev1.RuntimeState, error) {
	b, err := os.ReadFile(d.statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &statev1.RuntimeState{}, nil
		}
		return nil, fmt.Errorf("read state: %w", err)
	}
	if len(b) == 0 {
		return &statev1.RuntimeState{}, nil
	}
	st := &statev1.RuntimeState{}
	if err := st.UnmarshalVT(b); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}
	return st, nil
}

func (d *disk) save(st *statev1.RuntimeState) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	b, err := st.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	return perm.WriteGroupReadable(d.statePath, b)
}

func unmarshalRevocations(revs []*admissionv1.SignedRevocation, trustBundle *admissionv1.TrustBundle) map[types.PeerKey]*admissionv1.SignedRevocation {
	out := make(map[types.PeerKey]*admissionv1.SignedRevocation, len(revs))
	for _, rev := range revs {
		subjectKey := types.PeerKeyFromBytes(rev.GetEntry().GetSubjectPub())
		if trustBundle != nil {
			if err := auth.VerifyRevocation(rev, trustBundle); err != nil {
				slog.Warn("skipping invalid revocation from disk", "subject", subjectKey.String(), "err", err)
				continue
			}
		}
		out[subjectKey] = rev
	}
	return out
}

// --- YAML migration (state.yaml → state.pb) ---

type yamlDiskState struct {
	Peers       []yamlDiskPeer       `yaml:"peers,omitempty"`
	Revocations []yamlDiskRevocation `yaml:"revocations,omitempty"`
}

type yamlDiskPeer struct {
	IdentityPublic     string   `yaml:"identityPublic"`
	LastAddr           string   `yaml:"lastAddr,omitempty"`
	ExternalIP         string   `yaml:"externalIP,omitempty"`
	Addresses          []string `yaml:"addresses,omitempty"`
	Port               uint32   `yaml:"port,omitempty"`
	ExternalPort       uint32   `yaml:"externalPort,omitempty"`
	PubliclyAccessible bool     `yaml:"publiclyAccessible,omitempty"`
}

type yamlDiskRevocation struct {
	SubjectPub string `yaml:"subjectPub"`
	Data       string `yaml:"data"`
}

func migrateYAMLToProto(yamlPath, protoPath string) (bool, error) {
	raw, err := os.ReadFile(yamlPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("read yaml state: %w", err)
	}

	var ys yamlDiskState
	if err := yaml.Unmarshal(raw, &ys); err != nil {
		return false, fmt.Errorf("unmarshal yaml state: %w", err)
	}

	st := &statev1.RuntimeState{}

	for _, p := range ys.Peers {
		pk, err := types.PeerKeyFromString(p.IdentityPublic)
		if err != nil {
			slog.Warn("skipping invalid peer key during migration", "key", p.IdentityPublic, "err", err)
			continue
		}
		st.Peers = append(st.Peers, &statev1.PeerState{
			IdentityPub:        pk[:],
			Addresses:          p.Addresses,
			Port:               p.Port,
			ExternalPort:       p.ExternalPort,
			LastAddr:           p.LastAddr,
			ExternalIp:         p.ExternalIP,
			PubliclyAccessible: p.PubliclyAccessible,
		})
	}

	for _, dr := range ys.Revocations {
		b, err := base64.StdEncoding.DecodeString(dr.Data)
		if err != nil {
			slog.Warn("skipping invalid revocation during migration", "subject", dr.SubjectPub, "err", err)
			continue
		}
		rev := &admissionv1.SignedRevocation{}
		if err := rev.UnmarshalVT(b); err != nil {
			slog.Warn("skipping undecodable revocation during migration", "subject", dr.SubjectPub, "err", err)
			continue
		}
		st.Revocations = append(st.Revocations, rev)
	}

	pb, err := st.MarshalVT()
	if err != nil {
		return false, fmt.Errorf("marshal proto state: %w", err)
	}

	if err := perm.WriteGroupReadable(protoPath, pb); err != nil {
		return false, fmt.Errorf("write proto state: %w", err)
	}

	if err := os.Rename(yamlPath, yamlPath+".bak"); err != nil {
		slog.Warn("failed to rename old yaml state", "err", err)
	}

	slog.Info("migrated state.yaml to state.pb")
	return true, nil
}

// --- consumed_invites.json migration ---

const consumedInvitesJSONFile = "consumed_invites.json"

type jsonConsumedInviteRecord struct {
	TokenID        string `json:"tokenID"`
	ExpiresAtUnix  int64  `json:"expiresAtUnix"`
	ConsumedAtUnix int64  `json:"consumedAtUnix"`
}

type jsonConsumedInviteState struct {
	Invites []jsonConsumedInviteRecord `json:"invites"`
}

func migrateConsumedInvitesJSON(pollenDir, statePath string) error {
	jsonPath := filepath.Join(pollenDir, consumedInvitesJSONFile)
	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read consumed invites: %w", err)
	}

	if len(bytes.TrimSpace(raw)) == 0 {
		_ = os.Remove(jsonPath)
		return nil
	}

	var js jsonConsumedInviteState
	if err := json.Unmarshal(raw, &js); err != nil {
		return fmt.Errorf("decode consumed invites: %w", err)
	}

	stateRaw, err := os.ReadFile(statePath)
	if err != nil {
		return fmt.Errorf("read state for consumed invite migration: %w", err)
	}

	st := &statev1.RuntimeState{}
	if len(stateRaw) > 0 {
		if err := st.UnmarshalVT(stateRaw); err != nil {
			return fmt.Errorf("unmarshal state for consumed invite migration: %w", err)
		}
	}

	for _, rec := range js.Invites {
		if rec.TokenID == "" {
			continue
		}
		st.ConsumedInvites = append(st.ConsumedInvites, &statev1.ConsumedInvite{
			TokenId:      rec.TokenID,
			ExpiryUnix:   rec.ExpiresAtUnix,
			ConsumedUnix: rec.ConsumedAtUnix,
		})
	}

	pb, err := st.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshal state after consumed invite migration: %w", err)
	}

	if err := perm.WriteGroupReadable(statePath, pb); err != nil {
		return fmt.Errorf("write state after consumed invite migration: %w", err)
	}

	if err := os.Remove(jsonPath); err != nil {
		slog.Warn("failed to remove old consumed_invites.json", "err", err)
	}

	slog.Info("migrated consumed_invites.json into state.pb")
	return nil
}
