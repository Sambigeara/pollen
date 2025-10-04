package peers

import (
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
)

type (
	PeerStaticPublicKey []byte
)

type PeerStore struct {
	*peerv1.PeerStore
	path string
	mu   sync.RWMutex
}

func Load(dir string) (*PeerStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	pinsPath := filepath.Join(dir, "peers.sha256")

	f, err := os.OpenFile(pinsPath, os.O_RDONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	ps := &peerv1.PeerStore{}
	if err := proto.Unmarshal(b, ps); err != nil {
		return nil, err
	}

	if ps.Peers == nil {
		ps.Peers = make(map[string]*peerv1.Known)
	}

	return &PeerStore{
		PeerStore: ps,
		path:      pinsPath,
	}, nil
}

func (s *PeerStore) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Create(s.path)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := proto.Marshal(s.PeerStore)
	if err != nil {
		return err
	}

	_, err = f.Write(b)

	return err
}

func (s *PeerStore) Add(peer *peerv1.Known) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Peers[encodeStaticPublicKey(peer.StaticKey)] = peer
}

func (s *PeerStore) Get(key PeerStaticPublicKey) (*peerv1.Known, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pub, ok := s.Peers[encodeStaticPublicKey(key)]
	return pub, ok
}

func (s *PeerStore) PromoteToPeer(key PeerStaticPublicKey, peerAddr string) {
	s.Add(&peerv1.Known{
		StaticKey: key,
		Addr:      peerAddr,
	})
}

func (s *PeerStore) GetAllKnown() []*peerv1.Known {
	s.mu.RLock()
	defer s.mu.RUnlock()

	known := make([]*peerv1.Known, 0, len(s.Peers))
	for _, k := range s.Peers {
		known = append(known, k)
	}

	return known
}

func encodeStaticPublicKey(raw PeerStaticPublicKey) string {
	return hex.EncodeToString(raw)
}
