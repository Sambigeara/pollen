package admission

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
)

var _ Store = (*impl)(nil)

const (
	invitesDir  = "invites"
	keyFilePerm = 0o600
	keyDirPerm  = 0o700
)

type Admission interface {
	// ConsumeToken validates and consumes a one-time invite token by ID.
	// Returns true if the token was valid and consumed and persisted.
	ConsumeToken(tokenID string) (ok bool, err error)
}

type Store interface {
	Admission
	Save() error
	AddInvite(inv *admissionv1.Invite)
}

type impl struct {
	*admissionv1.InviteStore
	path string
	mu   sync.RWMutex
}

func Load(pollenDir string) (Store, error) {
	dir := filepath.Join(pollenDir, invitesDir)

	if err := os.MkdirAll(dir, keyDirPerm); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, "invites.sha256")

	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, keyFilePerm)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	is := &admissionv1.InviteStore{}
	if err := proto.Unmarshal(b, is); err != nil {
		return nil, err
	}

	if is.Invites == nil {
		is.Invites = make(map[string]*admissionv1.Invite)
	}

	return &impl{
		InviteStore: is,
		path:        path,
	}, nil
}

func (s *impl) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *impl) saveLocked() error {
	tmpPath := s.path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, keyFilePerm)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	b, err := proto.Marshal(s.InviteStore)
	if err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if _, err := f.Write(b); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if err := f.Sync(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, s.path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	return nil
}

func (s *impl) AddInvite(inv *admissionv1.Invite) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Invites[inv.Id] = inv
}

// ConsumeToken validates and consumes a one-time invite by ID.
func (s *impl) ConsumeToken(id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inv, ok := s.Invites[id]
	if !ok {
		return false, nil
	}

	delete(s.Invites, id)

	if err := s.saveLocked(); err != nil {
		s.Invites[id] = inv
		return false, err
	}

	return true, nil
}
