package transport

import (
	"context"
	"net"
)

var _ Transport = (*impl)(nil)

type Transport interface {
	Recv(ctx context.Context) (src string, b []byte, err error) // src is "ip:port"
	Send(dst string, b []byte) error
	LocalAddrs() []string
	Close() error
}

type impl struct {
	conn       *net.UDPConn
	localAddrs []string
}

func NewTransport(port int) (Transport, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: port})
	if err != nil {
		return nil, err
	}

	localAddresses, err := GetAdvertisableAddrs()
	if err != nil {
		return nil, err
	}

	return &impl{
		conn:       conn,
		localAddrs: localAddresses,
	}, nil
}

func (i *impl) Recv(ctx context.Context) (string, []byte, error) {
	buf := make([]byte, 2048)
	n, addr, err := i.conn.ReadFromUDP(buf)
	if err != nil {
		return "", nil, err
	}

	return addr.String(), buf[:n], nil
}

func (i *impl) Send(dst string, b []byte) error {
	addr, err := net.ResolveUDPAddr("udp", dst)
	if err != nil {
		return nil
	}

	if _, err = i.conn.WriteToUDP(b, addr); err != nil {
		return err
	}

	return nil
}

func (i *impl) LocalAddrs() []string {
	return i.localAddrs
}

func (i *impl) Close() error {
	return i.conn.Close()
}
