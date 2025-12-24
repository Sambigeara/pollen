package node

import (
	"crypto/ed25519"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

type Directory struct {
	cluster *state.Cluster
}

// IdentityPub maps a peer's Noise static public key to their Ed25519 identity public key.
func (d *Directory) IdentityPub(key types.PeerKey) (ed25519.PublicKey, bool) {
	rec, ok := d.cluster.Nodes.Get(key)
	if !ok || rec.Tombstone || rec.Value == nil || rec.Value.Keys == nil {
		return nil, false
	}

	pub := rec.Value.Keys.IdentityPub
	if len(pub) != ed25519.PublicKeySize {
		return nil, false
	}

	return pub, true
	// defensive copy
	// return ed25519.PublicKey(append([]byte(nil), pub...)), true
}
