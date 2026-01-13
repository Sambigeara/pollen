package transport

import (
	"fmt"
	"net"
)

var _ Transport = (*impl)(nil)

type Transport interface {
	Recv() (src string, b []byte, err error) // src is "ip:port"
	Send(dst string, b []byte) error
	Close() error
}

type impl struct {
	conn *net.UDPConn
}

func NewTransport(port int) (Transport, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: nil, Port: port})
	if err != nil {
		fmt.Printf("Failed to bind: %v\n", err)
		return nil, err
	}

	return &impl{
		conn: conn,
	}, nil
}

func (i *impl) Recv() (string, []byte, error) {
	buf := make([]byte, 2048) //nolint:mnd
	n, addr, err := i.conn.ReadFromUDP(buf)
	if err != nil {
		return "", nil, err
	}

	return addr.String(), buf[:n], nil
}

func (i *impl) Send(dst string, b []byte) error {
	addr, err := net.ResolveUDPAddr("udp", dst)
	if err != nil {
		return err
	}

	if _, err = i.conn.WriteToUDP(b, addr); err != nil {
		return err
	}

	return nil
}

func (i *impl) Close() error {
	return i.conn.Close()
}
