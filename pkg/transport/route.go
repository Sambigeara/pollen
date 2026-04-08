package transport

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/quic-go/quic-go"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
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

const datagramRouteHeaderSize = routeHeaderSize // 66 bytes: [32:dest][32:source][1:TTL][1:innerType]

func (m *QUICTransport) sendRoutedDatagram(ctx context.Context, dest types.PeerKey, innerType DatagramType, data []byte, nextHop types.PeerKey) error {
	hdr := make([]byte, 1+datagramRouteHeaderSize+len(data))
	hdr[0] = byte(DatagramTypeRouted)
	copy(hdr[1:33], dest[:])
	copy(hdr[33:65], m.localKey[:])
	hdr[65] = defaultRouteTTL
	hdr[66] = byte(innerType)
	copy(hdr[67:], data)
	return m.sendRawDatagram(ctx, nextHop, hdr)
}

func (m *QUICTransport) handleRoutedDatagram(ctx context.Context, payload []byte, upstreamPeer types.PeerKey) {
	if len(payload) < datagramRouteHeaderSize {
		return
	}

	var dest, source types.PeerKey
	copy(dest[:], payload[0:32])
	copy(source[:], payload[32:64])
	ttl := payload[64]
	innerType := DatagramType(payload[65])
	innerPayload := payload[datagramRouteHeaderSize:]

	// TODO(saml) no-op trafficTracker would probably be a better pattern here (and poss elsewhere)
	if m.trafficTracker != nil {
		m.trafficTracker.Record(upstreamPeer, uint64(len(payload)), 0)
	}

	if dest == m.localKey {
		switch innerType {
		case DatagramTypeTunnel:
			select {
			case m.tunnelDatagramCh <- Packet{From: source, Data: innerPayload}:
			case <-ctx.Done():
			}
		case DatagramTypeMembership:
			m.deliverRoutedMembershipDatagram(ctx, source, innerPayload)
		default:
		}
		return
	}

	if ttl <= 1 {
		return
	}

	m.sessionsMu.RLock()
	s, ok := m.sessions[dest]
	m.sessionsMu.RUnlock()
	nextHop := dest

	if !ok {
		if m.router == nil {
			return
		}
		nextHop, ok = m.router.NextHop(dest)
		if !ok || nextHop == source {
			return
		}
		m.sessionsMu.RLock()
		_, ok = m.sessions[nextHop]
		m.sessionsMu.RUnlock()
		if !ok {
			return
		}
	}
	_ = s // used only for the session existence check above

	fwd := make([]byte, 1+datagramRouteHeaderSize+len(innerPayload))
	fwd[0] = byte(DatagramTypeRouted)
	copy(fwd[1:33], dest[:])
	copy(fwd[33:65], source[:])
	fwd[65] = ttl - 1
	fwd[66] = byte(innerType)
	copy(fwd[67:], innerPayload)

	if m.trafficTracker != nil {
		m.trafficTracker.Record(nextHop, 0, uint64(len(fwd)))
	}

	_ = m.sendRawDatagram(ctx, nextHop, fwd)
}

func (m *QUICTransport) deliverRoutedMembershipDatagram(ctx context.Context, source types.PeerKey, data []byte) {
	env := &meshv1.Envelope{}
	if err := env.UnmarshalVT(data); err != nil {
		return
	}
	if resp, ok := env.GetBody().(*meshv1.Envelope_CertRenewalResponse); ok {
		select {
		case m.renewalCh <- resp.CertRenewalResponse:
		case <-ctx.Done():
		}
		return
	}
	if resp, ok := env.GetBody().(*meshv1.Envelope_CertPushResponse); ok {
		select {
		case m.certPushCh <- resp.CertPushResponse:
		case <-ctx.Done():
		}
		return
	}
	select {
	case m.recvCh <- Packet{From: source, Data: data}:
	case <-ctx.Done():
	}
}
