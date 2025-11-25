package mesh

import (
	"encoding/binary"
	"fmt"
	"net"
)

type messageType uint32

const (
	messageTypeHandshakeXXPsk2Init messageType = iota
	messageTypeHandshakeXXPsk2Resp
	messageTypeHandshakeIKInit
	messageTypeHandshakeIKResp

	messageTypeTransportData
	messageTypePing

	messageTypeTCPTunnelRequest
	messageTypeTCPTunnelResponse
)

type datagram struct {
	senderUDPAddr *net.UDPAddr
	msg           []byte
	tp            messageType
	senderID      uint32
	receiverID    uint32
}

func write(conn *UDPConn, addr *net.UDPAddr, tp messageType, senderID, receiverID uint32, msg []byte) error {
	var datagram []byte
	switch tp {
	case messageTypePing:
		datagram = make([]byte, 4)
		binary.BigEndian.PutUint32(datagram[:4], uint32(tp))
	case messageTypeHandshakeIKInit, messageTypeHandshakeXXPsk2Init:
		datagram = make([]byte, 8+len(msg))
		binary.BigEndian.PutUint32(datagram[:4], uint32(tp))
		binary.BigEndian.PutUint32(datagram[4:8], senderID)
		copy(datagram[8:], msg)
	case messageTypeHandshakeIKResp, messageTypeHandshakeXXPsk2Resp:
		datagram = make([]byte, 12+len(msg))
		binary.BigEndian.PutUint32(datagram[:4], uint32(tp))
		binary.BigEndian.PutUint32(datagram[4:8], senderID)
		binary.BigEndian.PutUint32(datagram[8:12], receiverID)
		copy(datagram[12:], msg)
	case messageTypeTransportData, messageTypeTCPTunnelRequest, messageTypeTCPTunnelResponse:
		datagram = make([]byte, 8+len(msg))
		binary.BigEndian.PutUint32(datagram[:4], uint32(tp))
		binary.BigEndian.PutUint32(datagram[4:8], receiverID)
		copy(datagram[8:], msg)
	}

	_, err := conn.WriteToUDP(datagram, addr)
	return err
}

func read(conn *UDPConn, buf []byte) (*datagram, error) {
	if buf == nil {
		buf = make([]byte, 2048)
	}
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}

	if n < 4 {
		return nil, fmt.Errorf("handshake frame too short: %d", n)
	}

	var senderID, receiverID uint32
	tp := messageType(binary.BigEndian.Uint32(buf[:4]))
	payloadOffset := 4
	switch tp {
	case messageTypePing:
	case messageTypeHandshakeIKInit, messageTypeHandshakeXXPsk2Init:
		if n < 8 {
			return nil, fmt.Errorf("handshake frame too short: %d", n)
		}
		senderID = binary.BigEndian.Uint32(buf[4:8])
		payloadOffset = 8
	case messageTypeHandshakeIKResp, messageTypeHandshakeXXPsk2Resp:
		if n < 12 {
			return nil, fmt.Errorf("handshake frame too short: %d", n)
		}
		senderID = binary.BigEndian.Uint32(buf[4:8])
		receiverID = binary.BigEndian.Uint32(buf[8:12])
		payloadOffset = 12
	case messageTypeTransportData, messageTypeTCPTunnelRequest, messageTypeTCPTunnelResponse:
		if n < 8 {
			return nil, fmt.Errorf("handshake frame too short: %d", n)
		}
		receiverID = binary.BigEndian.Uint32(buf[4:8])
		payloadOffset = 8
	}

	return &datagram{
		tp:            tp,
		senderID:      senderID,
		receiverID:    receiverID,
		senderUDPAddr: addr,
		msg:           append([]byte(nil), buf[payloadOffset:n]...), // copy to decouple from shared buffer
	}, nil
}
