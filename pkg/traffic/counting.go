package traffic

import (
	"io"

	"github.com/sambigeara/pollen/pkg/types"
)

// countedStream wraps an io.ReadWriteCloser, counting bytes read as inbound
// and bytes written as outbound traffic for a peer.
type countedStream struct {
	inner    io.ReadWriteCloser
	recorder Recorder
	peer     types.PeerKey
}

func (s *countedStream) Read(p []byte) (int, error) {
	n, err := s.inner.Read(p)
	if n > 0 {
		s.recorder.Record(s.peer, uint64(n), 0)
	}
	return n, err
}

func (s *countedStream) Write(p []byte) (int, error) {
	n, err := s.inner.Write(p)
	if n > 0 {
		s.recorder.Record(s.peer, 0, uint64(n))
	}
	return n, err
}

func (s *countedStream) Close() error { return s.inner.Close() }

func (s *countedStream) CloseWrite() error {
	if cw, ok := s.inner.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return s.inner.Close()
}

// WrapStream wraps a bidirectional stream with traffic counting.
// Bytes read count as inbound from peer; bytes written count as outbound to peer.
func WrapStream(stream io.ReadWriteCloser, recorder Recorder, peer types.PeerKey) io.ReadWriteCloser {
	return &countedStream{
		inner:    stream,
		recorder: recorder,
		peer:     peer,
	}
}
