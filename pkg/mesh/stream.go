package mesh

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/sambigeara/pollen/pkg/traffic"
	"github.com/sambigeara/pollen/pkg/types"
)

// stream wraps a QUIC stream to implement the Stream interface with safe Close
// semantics: CancelRead on the read side and Close (FIN) on the write side.
type stream struct{ *quic.Stream }

func (s stream) CloseWrite() error { return s.Stream.Close() }

func (s stream) Close() error {
	s.CancelRead(0)
	return s.Stream.Close()
}

const routeBufSize = 64 * 1024

var routeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, routeBufSize)
		return &b
	},
}

func routeChanged(r Router) <-chan struct{} {
	type notifier interface{ Changed() <-chan struct{} }
	if n, ok := r.(notifier); ok {
		return n.Changed()
	}
	return nil
}

// openStreamWaitLoop is the shared retry loop for opening tunnel, artifact, and
// workload streams. It tries a direct session first, then a routed path if a
// router is configured, and waits for either to become available.
func (m *impl) openStreamWaitLoop(ctx context.Context, peerKey types.PeerKey, streamType byte) (io.ReadWriteCloser, error) {
	for {
		sessionCh := m.sessions.onChange()
		routeCh := routeChanged(m.router)

		if _, ok := m.sessions.get(peerKey); ok {
			return m.openTypedStream(ctx, peerKey, streamType)
		}

		if m.router != nil {
			if nextHop, ok := m.router.Lookup(peerKey); ok {
				if _, ok := m.sessions.get(nextHop); ok {
					return m.openRoutedStream(ctx, peerKey, streamType, nextHop)
				}
			}
		}

		select {
		case <-sessionCh:
		case <-routeCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (m *impl) OpenStream(ctx context.Context, peerKey types.PeerKey) (io.ReadWriteCloser, error) {
	return m.openStreamWaitLoop(ctx, peerKey, streamTypeTunnel)
}

func (m *impl) OpenArtifactStream(ctx context.Context, peerKey types.PeerKey) (io.ReadWriteCloser, error) {
	return m.openStreamWaitLoop(ctx, peerKey, streamTypeArtifact)
}

func (m *impl) OpenWorkloadStream(ctx context.Context, peerKey types.PeerKey) (io.ReadWriteCloser, error) {
	return m.openStreamWaitLoop(ctx, peerKey, streamTypeWorkload)
}

func (m *impl) OpenClockStream(ctx context.Context, peerKey types.PeerKey) (Stream, error) {
	rwc, err := m.openTypedStream(ctx, peerKey, streamTypeClock)
	if err != nil {
		return nil, err
	}
	return rwc.(Stream), nil //nolint:forcetypeassert
}

func (m *impl) AcceptStream(ctx context.Context) (types.PeerKey, io.ReadWriteCloser, error) {
	return m.acceptFromCh(ctx, m.streamCh)
}

func (m *impl) AcceptClockStream(ctx context.Context) (types.PeerKey, io.ReadWriteCloser, error) {
	return m.acceptFromCh(ctx, m.clockStreamCh)
}

func (m *impl) openTypedStream(ctx context.Context, peerKey types.PeerKey, streamType byte) (io.ReadWriteCloser, error) {
	s, err := m.sessions.waitFor(ctx, peerKey)
	if err != nil {
		return nil, err
	}
	qs, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := qs.Write([]byte{streamType}); err != nil {
		qs.CancelWrite(0)
		qs.CancelRead(0)
		return nil, err
	}
	return stream{qs}, nil
}

func (m *impl) acceptFromCh(ctx context.Context, ch <-chan incomingStream) (types.PeerKey, io.ReadWriteCloser, error) {
	select {
	case incoming, ok := <-ch:
		if !ok {
			return types.PeerKey{}, nil, net.ErrClosed
		}
		return incoming.peerKey, incoming.stream, nil
	case <-ctx.Done():
		return types.PeerKey{}, nil, ctx.Err()
	}
}

func (m *impl) openRoutedStream(ctx context.Context, dest types.PeerKey, innerType byte, nextHop types.PeerKey) (io.ReadWriteCloser, error) {
	s, ok := m.sessions.get(nextHop)
	if !ok {
		return nil, fmt.Errorf("no session to next hop %s", nextHop.Short())
	}
	qs, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	var header [1 + routeHeaderSize]byte
	header[0] = streamTypeRouted
	copy(header[1:33], dest[:])
	copy(header[33:65], m.localKey[:])
	header[65] = defaultRouteTTL
	header[66] = innerType
	if _, err := qs.Write(header[:]); err != nil {
		qs.CancelWrite(0)
		qs.CancelRead(0)
		return nil, err
	}
	return stream{qs}, nil
}

func (m *impl) handleRoutedStream(ctx context.Context, qs *quic.Stream, upstreamPeer types.PeerKey) {
	var header [routeHeaderSize]byte
	if _, err := io.ReadFull(qs, header[:]); err != nil {
		qs.CancelRead(0)
		qs.CancelWrite(0)
		return
	}

	var dest, source types.PeerKey
	copy(dest[:], header[0:32])
	copy(source[:], header[32:64])
	ttl := header[64]
	innerType := header[65]

	if dest == m.localKey {
		m.deliverRoutedStream(ctx, qs, source, innerType)
		return
	}

	if ttl <= 1 {
		m.log.Debugw("routed stream TTL exhausted", "dest", dest.Short(), "source", source.Short())
		qs.CancelRead(0)
		qs.CancelWrite(0)
		return
	}

	m.forwardRoutedStream(ctx, qs, dest, source, ttl-1, innerType, upstreamPeer)
}

// deliverRoutedStream dispatches a routed stream that has arrived at its final
// destination. Tunnel, artifact, and workload streams are accepted; clock/gossip
// streams must not travel routed paths (gossip already propagates transitively
// via direct peers).
//
// NOTE: The source peerKey is asserted by the routing header, NOT authenticated
// by the QUIC session. Today this is acceptable because the tunnel manager uses
// peerKey only for logging. If future code makes authorization decisions based
// on AcceptStream's peerKey, routed streams must carry end-to-end proof of
// origin (e.g., a signature over the stream header).
func (m *impl) deliverRoutedStream(ctx context.Context, qs *quic.Stream, source types.PeerKey, innerType byte) {
	switch innerType {
	case streamTypeTunnel:
		select {
		case m.streamCh <- incomingStream{peerKey: source, stream: stream{qs}}:
		case <-ctx.Done():
			qs.CancelRead(0)
			qs.CancelWrite(0)
		default:
			qs.CancelRead(0)
			qs.CancelWrite(0)
		}
	case streamTypeArtifact:
		if h := m.artifactHandler; h != nil {
			go h(stream{qs}, source)
		} else {
			qs.CancelRead(0)
			qs.CancelWrite(0)
		}
	case streamTypeWorkload:
		if h := m.workloadHandler; h != nil {
			go h(stream{qs}, source)
		} else {
			qs.CancelRead(0)
			qs.CancelWrite(0)
		}
	default:
		qs.CancelRead(0)
		qs.CancelWrite(0)
	}
}

func (m *impl) forwardRoutedStream(ctx context.Context, inbound *quic.Stream, dest, source types.PeerKey, ttl, innerType byte, upstreamPeer types.PeerKey) {
	// Find next hop: try direct session first, then router.
	nextHop := dest
	s, ok := m.sessions.get(dest)
	if !ok {
		if m.router == nil {
			m.log.Debugw("no route to forward", "dest", dest.Short())
			inbound.CancelRead(0)
			inbound.CancelWrite(0)
			return
		}
		nextHop, ok = m.router.Lookup(dest)
		if !ok {
			m.log.Debugw("no route to forward", "dest", dest.Short())
			inbound.CancelRead(0)
			inbound.CancelWrite(0)
			return
		}
		if nextHop == source {
			m.log.Debugw("routing loop detected", "dest", dest.Short(), "source", source.Short())
			inbound.CancelRead(0)
			inbound.CancelWrite(0)
			return
		}
		s, ok = m.sessions.get(nextHop)
		if !ok {
			m.log.Debugw("no session to next hop", "nextHop", nextHop.Short())
			inbound.CancelRead(0)
			inbound.CancelWrite(0)
			return
		}
	}

	outbound, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		m.log.Debugw("open outbound routed stream failed", "nextHop", nextHop.Short(), "err", err)
		inbound.CancelRead(0)
		inbound.CancelWrite(0)
		return
	}

	// Write routing header on outbound stream.
	var header [1 + routeHeaderSize]byte
	header[0] = streamTypeRouted
	copy(header[1:33], dest[:])
	copy(header[33:65], source[:])
	header[65] = ttl
	header[66] = innerType
	if _, err := outbound.Write(header[:]); err != nil {
		outbound.CancelWrite(0)
		outbound.CancelRead(0)
		inbound.CancelRead(0)
		inbound.CancelWrite(0)
		return
	}

	in := traffic.WrapStream(stream{inbound}, m.trafficTracker, upstreamPeer)
	out := traffic.WrapStream(stream{outbound}, m.trafficTracker, nextHop)
	bridgeStreams(in, out)
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
