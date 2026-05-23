// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

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

	frame := make([]byte, 1+routeHeaderSize)
	frame[0] = byte(StreamTypeRouted)
	header := frame[1:]
	writeRouteHeader(header, dest, m.localKey, byte(innerType))
	if err := m.routeAuth.seal(routeDomainStream, header, nil); err != nil {
		cancelStream(stream)
		return Stream{}, err
	}

	if _, err := stream.Write(frame); err != nil {
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
	copy(dest[:], header[roDest:roSource])
	copy(source[:], header[roSource:roTTL])
	ttl := header[roTTL]
	innerType := StreamType(header[roInnerType])

	if dest == m.localKey {
		if _, ok := m.routeAuth.verify(routeDomainStream, header[:], nil); !ok {
			cancelStream(stream)
			return
		}
		switch innerType {
		case StreamTypeTunnel, StreamTypeBlob, StreamTypeBlobPlaintext, StreamTypeWorkload, StreamTypeMembership:
			select {
			case m.acceptCh <- acceptedStream{stream: Stream{stream}, stype: innerType, peerKey: source}:
			case <-ctx.Done():
				cancelStream(stream)
			}
		default:
			cancelStream(stream)
		}
		return
	}

	if ttl <= 1 {
		cancelStream(stream)
		return
	}

	m.forwardRoutedStream(ctx, stream, header[:], dest, source, ttl-1, upstreamPeer)
}

func (m *QUICTransport) forwardRoutedStream(ctx context.Context, inbound *quic.Stream, header []byte, dest, source types.PeerKey, ttl byte, upstreamPeer types.PeerKey) {
	if m.relayPermit != nil && !m.relayPermit(upstreamPeer) {
		cancelStream(inbound)
		return
	}
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

	frame := make([]byte, 1+routeHeaderSize)
	frame[0] = byte(StreamTypeRouted)
	copy(frame[1:], header)
	frame[1+roTTL] = ttl

	if _, err := outbound.Write(frame); err != nil {
		cancelStream(outbound)
		cancelStream(inbound)
		return
	}

	var in io.ReadWriteCloser = Stream{inbound}
	var out io.ReadWriteCloser = Stream{outbound}
	if m.trafficTracker != nil {
		in = WrapTrafficStream(in, m.trafficTracker, upstreamPeer)
		out = WrapTrafficStream(out, m.trafficTracker, nextHop)
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

func WrapTrafficStream(stream io.ReadWriteCloser, recorder TrafficRecorder, peer types.PeerKey) io.ReadWriteCloser {
	if recorder == nil {
		return stream
	}
	return &trafficCountedStream{inner: stream, recorder: recorder, peer: peer}
}

func (m *QUICTransport) sendRoutedDatagram(ctx context.Context, dest types.PeerKey, innerType DatagramType, data []byte, nextHop types.PeerKey) error {
	frame := make([]byte, 1+routeHeaderSize+len(data))
	frame[0] = byte(DatagramTypeRouted)
	header := frame[1 : 1+routeHeaderSize]
	writeRouteHeader(header, dest, m.localKey, byte(innerType))
	copy(frame[1+routeHeaderSize:], data)
	if err := m.routeAuth.seal(routeDomainDatagram, header, data); err != nil {
		return err
	}
	return m.sendRawDatagram(ctx, nextHop, frame)
}

func (m *QUICTransport) handleRoutedDatagram(ctx context.Context, payload []byte, upstreamPeer types.PeerKey) {
	if len(payload) < routeHeaderSize {
		return
	}
	header := payload[:routeHeaderSize]
	innerPayload := payload[routeHeaderSize:]

	var dest, source types.PeerKey
	copy(dest[:], header[roDest:roSource])
	copy(source[:], header[roSource:roTTL])
	ttl := header[roTTL]
	innerType := DatagramType(header[roInnerType])

	if m.trafficTracker != nil {
		m.trafficTracker.Record(upstreamPeer, uint64(len(payload)), 0)
	}

	if dest == m.localKey {
		if _, ok := m.routeAuth.verify(routeDomainDatagram, header, innerPayload); !ok {
			return
		}
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

	if m.relayPermit != nil && !m.relayPermit(upstreamPeer) {
		return
	}

	m.sessionsMu.RLock()
	_, ok := m.sessions[dest]
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

	fwd := make([]byte, 1+len(payload))
	fwd[0] = byte(DatagramTypeRouted)
	copy(fwd[1:], payload)
	fwd[1+roTTL] = ttl - 1

	if m.trafficTracker != nil {
		m.trafficTracker.Record(nextHop, 0, uint64(len(fwd)))
	}

	_ = m.sendRawDatagram(ctx, nextHop, fwd)
}

func (m *QUICTransport) deliverRoutedMembershipDatagram(ctx context.Context, source types.PeerKey, data []byte) {
	select {
	case m.recvCh <- Packet{From: source, Data: data}:
	case <-ctx.Done():
	}
}
