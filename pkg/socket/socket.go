package socket

import (
	"net"

	"github.com/sambigeara/pollen/pkg/types"
)

type Socket interface {
	LocalAddr() net.Addr
	Send(dst string, b []byte) error
	Recv() (src string, b []byte, err error)
	Close() error
	IsClosed() bool
}

type SocketEvent struct {
	Socket  Socket
	Src     string
	Payload []byte
}

type SocketStore interface {
	Base() Socket
	CreateEphemeral() (Socket, error)
	CreateBatch(count int) ([]Socket, error)
	CloseEphemeral()
	Close() error
	Events() <-chan SocketEvent
	AssociatePeerSocket(peerKey types.PeerKey, socket Socket)
	GetPeerSocket(peerKey types.PeerKey) (Socket, bool)
	SendToPeer(peerKey types.PeerKey, dst string, b []byte) error
}
