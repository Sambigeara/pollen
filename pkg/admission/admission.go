package admission

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
)

var _ Store = (*impl)(nil)

const invitesDir = "invites"

type Admission interface {
	ConsumePSK(tokenID string) (psk [32]byte, ok bool)
}

type Store interface {
	Admission
	Save() error
	AddInvite(inv *peerv1.Invite)
}

type impl struct {
	*peerv1.InviteStore
	path string
	mu   sync.RWMutex
}

func Load(pollenDir string) (Store, error) {
	dir := filepath.Join(pollenDir, invitesDir)

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, "invites.sha256")

	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	is := &peerv1.InviteStore{}
	if err := proto.Unmarshal(b, is); err != nil {
		return nil, err
	}

	if is.Invites == nil {
		is.Invites = make(map[string]*peerv1.Invite)
	}

	return &impl{
		InviteStore: is,
		path:        path,
	}, nil
}

func (s *impl) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Create(s.path)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := proto.Marshal(s.InviteStore)
	if err != nil {
		return err
	}

	_, err = f.Write(b)

	return err
}

func (s *impl) AddInvite(inv *peerv1.Invite) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Invites[inv.Id] = inv
}

func (s *impl) ConsumePSK(id string) ([32]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inv, ok := s.Invites[id]
	if ok {
		delete(s.Invites, id)
	}
	return [32]byte(inv.Psk), ok
}
