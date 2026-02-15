package node

import (
	"crypto/ed25519"

	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
)

type Directory struct {
	store *store.Store
}

func NewDirectory(s *store.Store) *Directory {
	return &Directory{store: s}
}

func (d *Directory) IdentityPub(key types.PeerKey) (ed25519.PublicKey, bool) {
	pub, ok := d.store.IdentityPub(key)
	if !ok {
		return nil, false
	}

	return ed25519.PublicKey(pub), true
}
