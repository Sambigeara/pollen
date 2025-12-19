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

type PeerEventKind int

const (
	PeerEventKindUp PeerEventKind = iota
	PeerEventKindDown
	// PeerEventKindRotated
	// PeerEventKindRekeyed.
)

type PeerEvent struct {
	Addr        string
	IdentityPub []byte
	Kind        PeerEventKind
	Peer        PeerKey
}

type Envelope struct {
	Payload []byte
	Type    MsgType
}
