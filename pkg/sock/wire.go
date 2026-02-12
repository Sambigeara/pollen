package sock

import (
	"encoding/binary"
	"fmt"

	"github.com/sambigeara/pollen/pkg/types"
)

type Frame struct {
	Src        string
	Payload    []byte
	Typ        types.MsgType
	SenderID   uint32
	ReceiverID uint32
}

//nolint:mnd
func decodeFrame(buf []byte, src string) (fr Frame, _ error) {
	n := len(buf)
	if n < 4 {
		return fr, fmt.Errorf("handshake frame too short: %d", n)
	}

	var senderID, receiverID uint32
	tp := types.MsgType(binary.BigEndian.Uint32(buf[:4]))
	payloadOffset := 4
	switch tp {
	case types.MsgTypePing:
	case types.MsgTypeHandshakeIKInit, types.MsgTypeHandshakeXXPsk2Init:
		if n < 8 {
			return fr, fmt.Errorf("handshake frame too short: %d", n)
		}
		senderID = binary.BigEndian.Uint32(buf[4:8])
		payloadOffset = 8
	case types.MsgTypeHandshakeIKResp, types.MsgTypeHandshakeXXPsk2Resp:
		if n < 12 {
			return fr, fmt.Errorf("handshake frame too short: %d", n)
		}
		senderID = binary.BigEndian.Uint32(buf[4:8])
		receiverID = binary.BigEndian.Uint32(buf[8:12])
		payloadOffset = 12
	case types.MsgTypeTransportData, types.MsgTypeTCPPunchRequest, types.MsgTypeTCPPunchTrigger,
		types.MsgTypeTCPPunchReady, types.MsgTypeTCPPunchResponse, types.MsgTypeGossip,
		types.MsgTypeUDPPunchCoordRequest, types.MsgTypeUDPPunchCoordResponse, types.MsgTypeDisconnect,
		types.MsgTypeTest, types.MsgTypeSessionRequest, types.MsgTypeSessionResponse,
		types.MsgTypeTCPPunchProbeRequest, types.MsgTypeTCPPunchProbeOffer, types.MsgTypeTCPPunchProbeResult,
		types.MsgTypeUDPRelay:
		if n < 8 {
			return fr, fmt.Errorf("handshake frame too short: %d", n)
		}
		receiverID = binary.BigEndian.Uint32(buf[4:8])
		payloadOffset = 8
	}

	return Frame{
		Src:        src,
		Typ:        tp,
		SenderID:   senderID,
		ReceiverID: receiverID,
		Payload:    buf[payloadOffset:n],
		// TODO(saml) If we ever start reusing buffers...
		// payload: append([]byte(nil), buf[payloadOffset:n]...),
	}, nil
}

//nolint:mnd
func encodeFrame(fr *Frame) []byte {
	var buf []byte

	switch fr.Typ {
	case types.MsgTypePing:
		buf = make([]byte, 4)
		binary.BigEndian.PutUint32(buf[:4], uint32(fr.Typ))
	case types.MsgTypeHandshakeIKInit, types.MsgTypeHandshakeXXPsk2Init:
		buf = make([]byte, 8+len(fr.Payload))
		binary.BigEndian.PutUint32(buf[:4], uint32(fr.Typ))
		binary.BigEndian.PutUint32(buf[4:8], fr.SenderID)
		copy(buf[8:], fr.Payload)
	case types.MsgTypeHandshakeIKResp, types.MsgTypeHandshakeXXPsk2Resp:
		buf = make([]byte, 12+len(fr.Payload))
		binary.BigEndian.PutUint32(buf[:4], uint32(fr.Typ))
		binary.BigEndian.PutUint32(buf[4:8], fr.SenderID)
		binary.BigEndian.PutUint32(buf[8:12], fr.ReceiverID)
		copy(buf[12:], fr.Payload)
	case types.MsgTypeTransportData, types.MsgTypeTCPPunchRequest, types.MsgTypeTCPPunchTrigger,
		types.MsgTypeTCPPunchReady, types.MsgTypeTCPPunchResponse, types.MsgTypeGossip,
		types.MsgTypeUDPPunchCoordRequest, types.MsgTypeUDPPunchCoordResponse, types.MsgTypeDisconnect,
		types.MsgTypeTest, types.MsgTypeSessionRequest, types.MsgTypeSessionResponse,
		types.MsgTypeTCPPunchProbeRequest, types.MsgTypeTCPPunchProbeOffer, types.MsgTypeTCPPunchProbeResult,
		types.MsgTypeUDPRelay:
		buf = make([]byte, 8+len(fr.Payload))
		binary.BigEndian.PutUint32(buf[:4], uint32(fr.Typ))
		binary.BigEndian.PutUint32(buf[4:8], fr.ReceiverID)
		copy(buf[8:], fr.Payload)
	}

	return buf
}
