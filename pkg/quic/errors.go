package quic

import "errors"

var (
	ErrConnClosed  = errors.New("connection closed")
	ErrNoPeer      = errors.New("no connection to peer")
	ErrUnreachable = errors.New("peer unreachable")
)
