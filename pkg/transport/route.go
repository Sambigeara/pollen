package transport

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/sambigeara/pollen/pkg/types"
)

func (m *impl) openRoutedStream(ctx context.Context, dest types.PeerKey, innerType StreamType, nextHop types.PeerKey) (Stream, error) {
	s, ok := m.sessions.get(nextHop)
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

func (m *impl) handleRoutedStream(ctx context.Context, stream *quic.Stream, upstreamPeer types.PeerKey) {
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
		m.deliverRoutedStream(ctx, stream, source, innerType)
		return
	}

	if ttl <= 1 {
		m.log.Debugw("routed stream TTL exhausted", "dest", dest.Short(), "source", source.Short())
		cancelStream(stream)
		return
	}

	m.forwardRoutedStream(ctx, stream, dest, source, ttl-1, innerType, upstreamPeer)
}

// NOTE: The source peerKey is asserted by the routing header, NOT authenticated
// by the QUIC session. If future code makes authorization decisions based on
// AcceptStream's peerKey, routed streams must carry end-to-end proof of origin.
func (m *impl) deliverRoutedStream(ctx context.Context, stream *quic.Stream, source types.PeerKey, innerType StreamType) {
	switch innerType {
	case StreamTypeTunnel, StreamTypeArtifact, StreamTypeWorkload:
		select {
		case m.acceptCh <- acceptedStream{stream: Stream{stream}, stype: innerType, peerKey: source}:
		case <-ctx.Done():
			cancelStream(stream)
		}
	default:
		cancelStream(stream)
	}
}

func (m *impl) forwardRoutedStream(ctx context.Context, inbound *quic.Stream, dest, source types.PeerKey, ttl byte, innerType StreamType, upstreamPeer types.PeerKey) {
	nextHop := dest
	s, ok := m.sessions.get(dest)
	if !ok {
		if m.router == nil {
			m.log.Debugw("no route to forward", "dest", dest.Short())
			cancelStream(inbound)
			return
		}
		nextHop, ok = m.router.NextHop(dest)
		if !ok {
			m.log.Debugw("no route to forward", "dest", dest.Short())
			cancelStream(inbound)
			return
		}
		if nextHop == source {
			m.log.Debugw("routing loop detected", "dest", dest.Short(), "source", source.Short())
			cancelStream(inbound)
			return
		}
		s, ok = m.sessions.get(nextHop)
		if !ok {
			m.log.Debugw("no session to next hop", "nextHop", nextHop.Short())
			cancelStream(inbound)
			return
		}
	}

	outbound, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		m.log.Debugw("open outbound routed stream failed", "nextHop", nextHop.Short(), "err", err)
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

const routeBufSize = 64 * 1024

var routeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, routeBufSize)
		return &b
	},
}

func bridgeStreams(c1, c2 io.ReadWriteCloser) {
	var wg sync.WaitGroup
	transfer := func(dst, src io.ReadWriteCloser) {
		bufPtr := routeBufPool.Get().(*[]byte) //nolint:forcetypeassert
		defer routeBufPool.Put(bufPtr)
		_, _ = io.CopyBuffer(dst, src, *bufPtr)
		meshCloseWrite(dst)
	}
	wg.Go(func() { transfer(c1, c2) })
	wg.Go(func() { transfer(c2, c1) })
	wg.Wait()
	_ = c1.Close()
	_ = c2.Close()
}

func meshCloseWrite(conn io.Closer) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
		return
	}
	_ = conn.Close()
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
