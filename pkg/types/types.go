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
	if len(b) != 32 {
		// TODO(saml) remove
		panic("IF I TRIGGER THEN PARTIAL KEYS ARE THE PROBLEM")
	}
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
