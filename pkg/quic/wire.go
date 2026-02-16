package quic

import (
	"fmt"

	"github.com/sambigeara/pollen/pkg/types"
)

func encodeDatagram(msgType types.MsgType, payload []byte) []byte {
	buf := make([]byte, 1+len(payload))
	buf[0] = byte(msgType)
	copy(buf[1:], payload)
	return buf
}

func decodeDatagram(data []byte) (types.MsgType, []byte, error) {
	if len(data) < 1 {
		return 0, nil, fmt.Errorf("datagram too short: %d bytes", len(data))
	}
	return types.MsgType(data[0]), data[1:], nil
}
