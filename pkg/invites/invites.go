package invites

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
)

const invitesDir = "invites"

type (
	InviteID string
)

type InviteStore struct {
	*peerv1.InviteStore
	path string
	mu   sync.RWMutex
}

func Load(pollenDir string) (*InviteStore, error) {
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

	return &InviteStore{
		InviteStore: is,
		path:        path,
	}, nil
}

func (s *InviteStore) Save() error {
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

func (s *InviteStore) AddInvite(inv *peerv1.Invite) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Invites[inv.Id] = inv
}

func (s *InviteStore) GetInvite(id InviteID) (*peerv1.Invite, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inv, ok := s.Invites[string(id)]
	return inv, ok
}

func (s *InviteStore) DeleteInvite(id InviteID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Invites, string(id))
}
