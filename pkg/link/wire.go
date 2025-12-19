package link

import (
	"encoding/binary"
	"fmt"

	"github.com/sambigeara/pollen/pkg/types"
)

type frame struct {
	payload    []byte
	typ        types.MsgType
	senderID   uint32
	receiverID uint32
}

func decodeFrame(buf []byte) (fr frame, _ error) {
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
	case types.MsgTypeTransportData, types.MsgTypeTCPTunnelRequest, types.MsgTypeTCPTunnelResponse, types.MsgTypeGossip, types.MsgTypeTest:
		if n < 8 {
			return fr, fmt.Errorf("handshake frame too short: %d", n)
		}
		receiverID = binary.BigEndian.Uint32(buf[4:8])
		payloadOffset = 8
	}

	return frame{
		typ:        tp,
		senderID:   senderID,
		receiverID: receiverID,
		payload:    buf[payloadOffset:n],
		// TODO(saml) If we ever start reusing buffers...
		// payload: append([]byte(nil), buf[payloadOffset:n]...),
	}, nil
}

func encodeFrame(fr *frame) []byte {
	var buf []byte

	switch fr.typ {
	case types.MsgTypePing:
		buf = make([]byte, 4)
		binary.BigEndian.PutUint32(buf[:4], uint32(fr.typ))
	case types.MsgTypeHandshakeIKInit, types.MsgTypeHandshakeXXPsk2Init:
		buf = make([]byte, 8+len(fr.payload))
		binary.BigEndian.PutUint32(buf[:4], uint32(fr.typ))
		binary.BigEndian.PutUint32(buf[4:8], fr.senderID)
		copy(buf[8:], fr.payload)
	case types.MsgTypeHandshakeIKResp, types.MsgTypeHandshakeXXPsk2Resp:
		buf = make([]byte, 12+len(fr.payload))
		binary.BigEndian.PutUint32(buf[:4], uint32(fr.typ))
		binary.BigEndian.PutUint32(buf[4:8], fr.senderID)
		binary.BigEndian.PutUint32(buf[8:12], fr.receiverID)
		copy(buf[12:], fr.payload)
	case types.MsgTypeTransportData, types.MsgTypeTCPTunnelRequest, types.MsgTypeTCPTunnelResponse, types.MsgTypeGossip, types.MsgTypeTest:
		buf = make([]byte, 8+len(fr.payload))
		binary.BigEndian.PutUint32(buf[:4], uint32(fr.typ))
		binary.BigEndian.PutUint32(buf[4:8], fr.receiverID)
		copy(buf[8:], fr.payload)
	}

	return buf
}
