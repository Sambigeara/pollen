package types

import (
	"bytes"
	"encoding/hex"
)

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

	MsgTypePunchCoordRequest
	MsgTypePunchCoordResponse

	MsgTypeGossip
	MsgTypeDisconnect
	MsgTypeTest
)

type PeerKey [32]byte // Noise static pub

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

func (pk *PeerKey) Bytes() []byte {
	return pk[:]
}

func (pk *PeerKey) String() string {
	return hex.EncodeToString(pk[:])
}

func (pk PeerKey) Less(other PeerKey) bool {
	return bytes.Compare(pk[:], other[:]) < 0
}

type Envelope struct {
	Payload []byte
	Type    MsgType
}
