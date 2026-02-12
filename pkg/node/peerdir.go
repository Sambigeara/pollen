package node

import (
	"crypto/ed25519"

	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
)

type Directory struct {
	store *store.Store
}

// IdentityPub maps a peer's Noise static public key to their Ed25519 identity public key.
func (d *Directory) IdentityPub(key types.PeerKey) (ed25519.PublicKey, bool) {
	pub, ok := d.store.IdentityPub(key)
	if !ok {
		return nil, false
	}
	if len(pub) != ed25519.PublicKeySize {
		return nil, false
	}

	return ed25519.PublicKey(pub), true
}
