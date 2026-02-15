package quic

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sambigeara/pollen/pkg/types"
)

// Datagram framing: [MsgType (1 byte)] [payload...]
// No session IDs (QUIC handles that), no encryption (QUIC handles that).

const (
	fragmentMarker     byte = 0x80
	fragmentHeaderSize int  = 10 // marker(1) + msgType(1) + msgID(4) + index(2) + total(2)
)

var errFragmentedDatagram = errors.New("fragmented datagram")

type datagramFragment struct {
	payload []byte
	msgType types.MsgType
	msgID   uint32
	index   uint16
	total   uint16
}

func encodeDatagram(msgType types.MsgType, payload []byte) []byte {
	buf := make([]byte, 1+len(payload))
	buf[0] = byte(msgType)
	copy(buf[1:], payload)
	return buf
}

func encodeFragmentedDatagrams(msgType types.MsgType, payload []byte, maxDatagramPayload int, msgID uint32) ([][]byte, error) {
	if maxDatagramPayload <= fragmentHeaderSize {
		return nil, fmt.Errorf("max datagram payload too small for fragmentation: %d", maxDatagramPayload)
	}

	chunkSize := maxDatagramPayload - fragmentHeaderSize
	if chunkSize <= 0 {
		return nil, fmt.Errorf("invalid chunk size: %d", chunkSize)
	}

	total := (len(payload) + chunkSize - 1) / chunkSize
	if total <= 0 {
		total = 1
	}
	if total > int(^uint16(0)) {
		return nil, fmt.Errorf("payload too large to fragment: %d bytes", len(payload))
	}

	frames := make([][]byte, 0, total)
	for idx := 0; idx < total; idx++ {
		start := idx * chunkSize
		end := min(start+chunkSize, len(payload))
		chunk := payload[start:end]

		frame := make([]byte, fragmentHeaderSize+len(chunk))
		frame[0] = fragmentMarker
		frame[1] = byte(msgType)
		binary.BigEndian.PutUint32(frame[2:6], msgID)
		binary.BigEndian.PutUint16(frame[6:8], uint16(idx))
		binary.BigEndian.PutUint16(frame[8:10], uint16(total))
		copy(frame[10:], chunk)
		frames = append(frames, frame)
	}

	return frames, nil
}

func decodeDatagram(data []byte) (types.MsgType, []byte, *datagramFragment, error) {
	if len(data) < 1 {
		return 0, nil, nil, fmt.Errorf("datagram too short: %d bytes", len(data))
	}

	if data[0] != fragmentMarker {
		return types.MsgType(data[0]), data[1:], nil, nil
	}

	if len(data) < fragmentHeaderSize {
		return 0, nil, nil, fmt.Errorf("fragmented datagram too short: %d bytes", len(data))
	}

	frag := &datagramFragment{
		msgType: types.MsgType(data[1]),
		msgID:   binary.BigEndian.Uint32(data[2:6]),
		index:   binary.BigEndian.Uint16(data[6:8]),
		total:   binary.BigEndian.Uint16(data[8:10]),
		payload: data[10:],
	}

	if frag.total == 0 {
		return 0, nil, nil, errors.New("fragmented datagram has zero total")
	}
	if frag.index >= frag.total {
		return 0, nil, nil, fmt.Errorf("fragment index out of range: %d/%d", frag.index, frag.total)
	}

	return 0, nil, frag, errFragmentedDatagram
}
