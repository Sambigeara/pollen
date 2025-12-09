package mesh

import (
	"encoding/binary"
	"fmt"
	"net"
)

type MessageType uint32

const (
	MessageTypeHandshakeXXPsk2Init MessageType = iota
	MessageTypeHandshakeXXPsk2Resp
	MessageTypeHandshakeIKInit
	MessageTypeHandshakeIKResp

	MessageTypeTransportData
	MessageTypePing

	MessageTypeTCPTunnelRequest
	MessageTypeTCPTunnelResponse

	MessageTypeGossip
	MessageTypeTest
)

type datagram struct {
	senderUDPAddr *net.UDPAddr
	msg           []byte
	tp            MessageType
	senderID      uint32
	receiverID    uint32
}

func write(conn *UDPConn, addr *net.UDPAddr, tp MessageType, senderID, receiverID uint32, msg []byte) error {
	var datagram []byte
	switch tp {
	case MessageTypePing:
		datagram = make([]byte, 4)
		binary.BigEndian.PutUint32(datagram[:4], uint32(tp))
	case MessageTypeHandshakeIKInit, MessageTypeHandshakeXXPsk2Init:
		datagram = make([]byte, 8+len(msg))
		binary.BigEndian.PutUint32(datagram[:4], uint32(tp))
		binary.BigEndian.PutUint32(datagram[4:8], senderID)
		copy(datagram[8:], msg)
	case MessageTypeHandshakeIKResp, MessageTypeHandshakeXXPsk2Resp:
		datagram = make([]byte, 12+len(msg))
		binary.BigEndian.PutUint32(datagram[:4], uint32(tp))
		binary.BigEndian.PutUint32(datagram[4:8], senderID)
		binary.BigEndian.PutUint32(datagram[8:12], receiverID)
		copy(datagram[12:], msg)
	case MessageTypeTransportData, MessageTypeTCPTunnelRequest, MessageTypeTCPTunnelResponse, MessageTypeGossip, MessageTypeTest:
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
	tp := MessageType(binary.BigEndian.Uint32(buf[:4]))
	payloadOffset := 4
	switch tp {
	case MessageTypePing:
	case MessageTypeHandshakeIKInit, MessageTypeHandshakeXXPsk2Init:
		if n < 8 {
			return nil, fmt.Errorf("handshake frame too short: %d", n)
		}
		senderID = binary.BigEndian.Uint32(buf[4:8])
		payloadOffset = 8
	case MessageTypeHandshakeIKResp, MessageTypeHandshakeXXPsk2Resp:
		if n < 12 {
			return nil, fmt.Errorf("handshake frame too short: %d", n)
		}
		senderID = binary.BigEndian.Uint32(buf[4:8])
		receiverID = binary.BigEndian.Uint32(buf[8:12])
		payloadOffset = 12
	case MessageTypeTransportData, MessageTypeTCPTunnelRequest, MessageTypeTCPTunnelResponse, MessageTypeGossip, MessageTypeTest:
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
