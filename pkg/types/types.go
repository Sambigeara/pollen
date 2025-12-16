package types

import "encoding/hex"

// 32 bytes for a standard Noise/Ed25519 public key
type NodeID [32]byte

func (n NodeID) String() string { return hex.EncodeToString(n[:]) }

func IDFromBytes(b []byte) (NodeID, bool) {
	var id NodeID
	if len(b) != 32 {
		return id, false
	}
	copy(id[:], b)
	return id, true
}
