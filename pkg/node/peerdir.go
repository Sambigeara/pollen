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
func (d *Directory) IdentityPub(peerNoisePub []byte) (ed25519.PublicKey, bool) {
	id := types.PeerKeyFromBytes(peerNoisePub)

	rec, ok := d.cluster.Nodes.Get(id)
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
