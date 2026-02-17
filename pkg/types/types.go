package types

import (
	"bytes"
	"encoding/hex"
)

type PeerKey [32]byte // ed25519 public key

func PeerKeyFromBytes(b []byte) PeerKey {
	var id PeerKey
	copy(id[:], b)
	return id
}

func PeerKeyFromString(s string) (PeerKey, error) {
	var id PeerKey
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	return PeerKeyFromBytes(b), nil
}

func (pk PeerKey) Bytes() []byte {
	return pk[:]
}

func (pk PeerKey) String() string {
	return hex.EncodeToString(pk[:])
}

func (pk PeerKey) Short() string {
	return pk.String()[:8]
}

func (pk PeerKey) Less(other PeerKey) bool {
	return bytes.Compare(pk[:], other[:]) < 0
}

type Envelope struct {
	Payload []byte
}
