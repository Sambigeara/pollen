package membership

import (
	"context"
	"fmt"
	"io"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/observability/traces"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *Service) gossip(ctx context.Context) []state.Event {
	digestBytes, err := s.store.Snapshot().Digest().Marshal()
	if err != nil {
		s.log.Warnw("failed to marshal digest", "err", err)
		return nil
	}

	var allEvents []state.Event
	s.mu.Lock()
	defer s.mu.Unlock() // Ensure concurrent append safety if used with inner routines

	for _, peerID := range s.mesh.ConnectedPeers() {
		if peerID == s.localID {
			continue
		}
		pID := peerID
		s.spawn(ctx, func(gossipCtx context.Context) {
			ctx, cancel := context.WithTimeout(gossipCtx, gossipStreamTimeout)
			defer cancel()
			events, err := s.sendDigestViaStream(ctx, pID, digestBytes)
			if err != nil {
				s.log.Debugw("digest gossip send failed", "peer", pID.Short(), "err", err)
				return
			}
			if len(events) > 0 {
				s.mu.Lock()
				allEvents = append(allEvents, events...)
				s.mu.Unlock()
			}
		})
	}
	return allEvents
}

func (s *Service) sendDigestViaStream(ctx context.Context, peerID types.PeerKey, digestBytes []byte) ([]state.Event, error) {
	stream, err := s.streams.OpenStream(ctx, peerID, transport.StreamTypeDigest)
	if err != nil {
		return nil, fmt.Errorf("open digest stream to %s: %w", peerID.Short(), err)
	}
	defer stream.Close()

	if _, err := stream.Write(digestBytes); err != nil {
		return nil, fmt.Errorf("write digest to %s: %w", peerID.Short(), err)
	}

	if err := stream.CloseWrite(); err != nil {
		return nil, fmt.Errorf("half-close digest stream to %s: %w", peerID.Short(), err)
	}

	resp, err := io.ReadAll(io.LimitReader(stream, maxResponseSize+1))
	if err != nil {
		return nil, fmt.Errorf("read digest response from %s: %w", peerID.Short(), err)
	}
	if len(resp) == 0 {
		return nil, nil
	}
	if len(resp) > maxResponseSize {
		return nil, fmt.Errorf("digest response from %s exceeded size limit (%d bytes)", peerID.Short(), len(resp))
	}

	events, _, err := s.store.ApplyDelta(peerID, resp)
	if err != nil {
		return nil, fmt.Errorf("apply digest response from %s: %w", peerID.Short(), err)
	}
	return events, nil
}

func (s *Service) HandleDigestStream(ctx context.Context, stream transport.Stream, from types.PeerKey) {
	defer stream.Close()
	_, span := s.tracer.Start(ctx, "gossip.handleDigest")
	span.SetAttributes(attribute.String("peer", from.Short()))
	defer span.End()

	const maxDigestSize = 256 << 10
	b, err := io.ReadAll(io.LimitReader(stream, maxDigestSize+1))
	if err != nil {
		s.log.Debugw("read digest stream failed", "peer", from.Short(), "err", err)
		return
	}
	if len(b) > maxDigestSize {
		s.log.Warnw("digest stream exceeded size limit", "peer", from.Short(), "size", len(b))
		return
	}

	digest, err := state.UnmarshalDigest(b)
	if err != nil {
		s.log.Debugw("unmarshal digest stream failed", "peer", from.Short(), "err", err)
		return
	}

	resp := s.store.EncodeDelta(digest)
	if len(resp) == 0 {
		return
	}
	if len(resp) > maxResponseSize {
		s.log.Warnw("digest response exceeded size limit", "peer", from.Short(), "size", len(resp))
		return
	}

	if _, err := stream.Write(resp); err != nil {
		s.log.Debugw("write digest response failed", "peer", from.Short(), "err", err)
	}
}

func (s *Service) handleDatagram(ctx context.Context, from types.PeerKey, data []byte) {
	env := &meshv1.Envelope{}
	if err := env.UnmarshalVT(data); err != nil {
		s.log.Debugw("unmarshal datagram failed", "peer", from.Short(), "err", err)
		return
	}
	switch body := env.GetBody().(type) {
	case *meshv1.Envelope_Events:
		sc := traces.SpanContextFromTraceID(env.GetTraceId())
		spanCtx := trace.ContextWithRemoteSpanContext(ctx, sc)
		_, span := s.tracer.Start(spanCtx, "gossip.applyDelta")
		span.SetAttributes(attribute.String("peer", from.Short()))
		defer span.End()

		batchData, err := body.Events.MarshalVT()
		if err != nil {
			s.log.Debugw("re-marshal events batch failed", "err", err)
			return
		}

		events, rebroadcast, err := s.store.ApplyDelta(from, batchData)
		if err != nil {
			s.log.Debugw("apply delta from datagram failed", "peer", from.Short(), "err", err)
			return
		}
		if len(rebroadcast) > 0 {
			s.broadcastBatchBytes(ctx, rebroadcast)
		}
		s.forwardEvents(events)

	case *meshv1.Envelope_ObservedAddress:
		s.handleObservedAddress(from, body.ObservedAddress)

	case *meshv1.Envelope_CertRenewalRequest:
		s.handleCertRenewalRequest(ctx, from, body.CertRenewalRequest)

	default:
		if s.datagramHandler != nil {
			s.datagramHandler(ctx, from, env)
		}
	}
}

func (s *Service) doEagerSync(ctx context.Context, peerKey types.PeerKey) {
	s.mu.Lock()
	s.lastEagerSync[peerKey] = time.Now()
	s.mu.Unlock()

	digestBytes, err := s.store.Snapshot().Digest().Marshal()
	if err != nil {
		s.log.Warnw("marshal digest for eager sync failed", "err", err)
		return
	}

	s.spawn(ctx, func(eagerCtx context.Context) {
		ctx, cancel := context.WithTimeout(eagerCtx, eagerSyncTimeout)
		defer cancel()
		events, err := s.sendDigestViaStream(ctx, peerKey, digestBytes)
		if err != nil {
			s.log.Debugw("eager sync failed", "peer", peerKey.Short(), "err", err)
			s.eagerSyncFailures.Add(1)
			return
		}
		s.eagerSyncs.Add(1)
		s.forwardEvents(events)
	})
}

func (s *Service) broadcastEvents(ctx context.Context, events []*statev1.GossipEvent) {
	if len(events) == 0 {
		return
	}
	s.broadcastGossipBatches(ctx, s.mesh.ConnectedPeers(), batchEvents(events, maxDatagramPayload))
}

func (s *Service) broadcastGossipBatches(ctx context.Context, peerIDs []types.PeerKey, batches [][]*statev1.GossipEvent) {
	failed := make(map[types.PeerKey]struct{})
	for _, batch := range batches {
		data, err := (&meshv1.Envelope{Body: &meshv1.Envelope_Events{
			Events: &statev1.GossipEventBatch{Events: batch},
		}}).MarshalVT()
		if err != nil {
			s.log.Debugw("gossip batch marshal failed", "err", err)
			continue
		}
		for _, peerID := range peerIDs {
			if _, skip := failed[peerID]; peerID == s.localID || skip {
				continue
			}
			if err := s.mesh.Send(ctx, peerID, data); err != nil {
				s.log.Debugw("event gossip send failed", "peer", peerID.Short(), "err", err)
				failed[peerID] = struct{}{}
			}
		}
	}
}

func (s *Service) broadcastBatchBytes(ctx context.Context, data []byte) {
	if len(data) == 0 {
		return
	}
	var batch statev1.GossipEventBatch
	if err := batch.UnmarshalVT(data); err != nil {
		s.log.Debugw("unmarshal gossip batch failed", "err", err)
		return
	}
	s.broadcastEvents(ctx, batch.GetEvents())
}

func batchEvents(events []*statev1.GossipEvent, maxSize int) [][]*statev1.GossipEvent {
	if len(events) == 0 {
		return nil
	}

	var batches [][]*statev1.GossipEvent
	var current []*statev1.GossipEvent
	currentSize := envelopeOverhead

	for _, event := range events {
		eventSize := (&statev1.GossipEventBatch{Events: []*statev1.GossipEvent{event}}).SizeVT()
		if len(current) > 0 && currentSize+eventSize > maxSize {
			batches = append(batches, current)
			current = nil
			currentSize = envelopeOverhead
		}
		current = append(current, event)
		currentSize += eventSize
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}
