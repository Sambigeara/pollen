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

const peersDir = "peers"
const peersFile = "peers.sha256"

type (
	PeerNoiseKey []byte
	PeerSig      []byte
)

type PeerStore struct {
	*peerv1.PeerStore
	path string
	mu   sync.RWMutex
}

func Load(pollenDir string) (*PeerStore, error) {
	dir := filepath.Join(pollenDir, peersDir)

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	pinsPath := filepath.Join(dir, peersFile)

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

	s.Peers[EncodeStaticPublicKey(peer.NoisePub)] = peer
}

func (s *PeerStore) Get(key PeerNoiseKey) (*peerv1.Known, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pub, ok := s.Peers[EncodeStaticPublicKey(key)]
	return pub, ok
}

func (s *PeerStore) PromoteToPeer(key PeerNoiseKey, sig PeerSig, peerAddr string) {
	s.Add(&peerv1.Known{
		NoisePub: key,
		SigPub:   sig,
		Addr:     peerAddr,
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

func EncodeStaticPublicKey(raw PeerNoiseKey) string {
	return hex.EncodeToString(raw)
}
