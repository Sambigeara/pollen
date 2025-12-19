package types

import "encoding/hex"

type MsgType uint32

const (
	MsgTypeHandshakeXXPsk2Init MsgType = iota
	MsgTypeHandshakeXXPsk2Resp
	MsgTypeHandshakeIKInit
	MsgTypeHandshakeIKResp

	MsgTypeTransportData
	MsgTypePing

	MsgTypeTCPTunnelRequest
	MsgTypeTCPTunnelResponse

	MsgTypeGossip
	MsgTypeTest
)

// TODO(saml) this is the same as the preexisting `NodeID` which I think I would like to swop back to after the refactor
type PeerKey [32]byte // Noise static pub

func PeerKeyFromBytes(b []byte) PeerKey {
	var id PeerKey
	copy(id[:], b)
	return id
}

func (pk *PeerKey) Bytes() []byte {
	return pk[:]
}

func (pk *PeerKey) String() string {
	return hex.EncodeToString(pk[:])
}

type PeerInfo struct {
	Peer  PeerKey
	Addrs []string // UDP addrs (hints)
	// Optional: identity pub, admin pub, etc. live in control-plane.
}

type PeerEventKind int

const (
	PeerEventKindUp PeerEventKind = iota
	PeerEventKindDown
	// PeerEventKindRotated
	// PeerEventKindRekeyed
)

type PeerEvent struct {
	Peer        PeerKey
	Addr        string        // the addr that “won” (optional)
	Kind        PeerEventKind // Up/Down/Rotated/Rekeyed...
	IdentityPub []byte
}

type Envelope struct {
	Type    MsgType
	Payload []byte
}
