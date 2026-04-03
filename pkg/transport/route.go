package transport

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/sambigeara/pollen/pkg/types"
)

func (m *QUICTransport) openRoutedStream(ctx context.Context, dest types.PeerKey, innerType StreamType, nextHop types.PeerKey) (Stream, error) {
	m.sessionsMu.RLock()
	s, ok := m.sessions[nextHop]
	m.sessionsMu.RUnlock()
	if !ok {
		return Stream{}, fmt.Errorf("no session to next hop %s", nextHop.Short())
	}
	stream, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return Stream{}, err
	}

	var header [1 + routeHeaderSize]byte
	header[0] = byte(StreamTypeRouted)
	copy(header[1:33], dest[:])
	copy(header[33:65], m.localKey[:])
	header[65] = defaultRouteTTL
	header[66] = byte(innerType)

	if _, err := stream.Write(header[:]); err != nil {
		cancelStream(stream)
		return Stream{}, err
	}
	return Stream{stream}, nil
}

func (m *QUICTransport) handleRoutedStream(ctx context.Context, stream *quic.Stream, upstreamPeer types.PeerKey) {
	var header [routeHeaderSize]byte
	if _, err := io.ReadFull(stream, header[:]); err != nil {
		cancelStream(stream)
		return
	}

	var dest, source types.PeerKey
	copy(dest[:], header[0:32])
	copy(source[:], header[32:64])
	ttl := header[64]
	innerType := StreamType(header[65])

	if dest == m.localKey {
		if innerType == StreamTypeTunnel || innerType == StreamTypeArtifact || innerType == StreamTypeWorkload {
			select {
			case m.acceptCh <- acceptedStream{stream: Stream{stream}, stype: innerType, peerKey: source}:
			case <-ctx.Done():
				cancelStream(stream)
			}
		} else {
			cancelStream(stream)
		}
		return
	}

	if ttl <= 1 {
		cancelStream(stream)
		return
	}

	m.forwardRoutedStream(ctx, stream, dest, source, ttl-1, innerType, upstreamPeer)
}

func (m *QUICTransport) forwardRoutedStream(ctx context.Context, inbound *quic.Stream, dest, source types.PeerKey, ttl byte, innerType StreamType, upstreamPeer types.PeerKey) {
	m.sessionsMu.RLock()
	s, ok := m.sessions[dest]
	m.sessionsMu.RUnlock()
	nextHop := dest

	if !ok {
		if m.router == nil {
			cancelStream(inbound)
			return
		}
		nextHop, ok = m.router.NextHop(dest)
		if !ok || nextHop == source {
			cancelStream(inbound)
			return
		}
		m.sessionsMu.RLock()
		s, ok = m.sessions[nextHop]
		m.sessionsMu.RUnlock()
		if !ok {
			cancelStream(inbound)
			return
		}
	}

	outbound, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		cancelStream(inbound)
		return
	}

	var header [1 + routeHeaderSize]byte
	header[0] = byte(StreamTypeRouted)
	copy(header[1:33], dest[:])
	copy(header[33:65], source[:])
	header[65] = ttl
	header[66] = byte(innerType)

	if _, err := outbound.Write(header[:]); err != nil {
		cancelStream(outbound)
		cancelStream(inbound)
		return
	}

	var in io.ReadWriteCloser = Stream{inbound}
	var out io.ReadWriteCloser = Stream{outbound}
	if m.trafficTracker != nil {
		in = wrapTrafficStream(in, m.trafficTracker, upstreamPeer)
		out = wrapTrafficStream(out, m.trafficTracker, nextHop)
	}
	bridgeStreams(in, out)
}

var routeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64*1024) //nolint:mnd
		return &b
	},
}

func bridgeStreams(c1, c2 io.ReadWriteCloser) {
	var wg sync.WaitGroup
	transfer := func(dst, src io.ReadWriteCloser) {
		bufPtr := routeBufPool.Get().(*[]byte) //nolint:forcetypeassert
		defer routeBufPool.Put(bufPtr)
		_, _ = io.CopyBuffer(dst, src, *bufPtr)
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		} else {
			_ = dst.Close()
		}
	}
	wg.Add(2) //nolint:mnd
	go func() { defer wg.Done(); transfer(c1, c2) }()
	go func() { defer wg.Done(); transfer(c2, c1) }()
	wg.Wait()
	_ = c1.Close()
	_ = c2.Close()
}

type trafficCountedStream struct {
	inner    io.ReadWriteCloser
	recorder TrafficRecorder
	peer     types.PeerKey
}

func (s *trafficCountedStream) Read(p []byte) (int, error) {
	n, err := s.inner.Read(p)
	if n > 0 {
		s.recorder.Record(s.peer, uint64(n), 0)
	}
	return n, err
}

func (s *trafficCountedStream) Write(p []byte) (int, error) {
	n, err := s.inner.Write(p)
	if n > 0 {
		s.recorder.Record(s.peer, 0, uint64(n))
	}
	return n, err
}

func (s *trafficCountedStream) Close() error { return s.inner.Close() }

func wrapTrafficStream(stream io.ReadWriteCloser, recorder TrafficRecorder, peer types.PeerKey) io.ReadWriteCloser {
	return &trafficCountedStream{inner: stream, recorder: recorder, peer: peer}
}
