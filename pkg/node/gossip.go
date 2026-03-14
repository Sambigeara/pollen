package node

import (
	"context"
	"fmt"
	"io"
	"sync"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

const maxResponseSize = 4 << 20 // 4 MB

func (n *Node) queueGossipEvents(events []*statev1.GossipEvent) {
	if len(events) == 0 {
		return
	}

	select {
	case n.gossipEvents <- events:
	default:
	}
}

func (n *Node) broadcastEvents(ctx context.Context, events []*statev1.GossipEvent) {
	if len(events) == 0 {
		return
	}

	n.broadcastGossipBatches(ctx, n.GetConnectedPeers(), batchEvents(events, MaxDatagramPayload))
}

func (n *Node) broadcastGossipBatches(ctx context.Context, peerIDs []types.PeerKey, batches [][]*statev1.GossipEvent) {
	failed := make(map[types.PeerKey]struct{})
	for _, batch := range batches {
		env := &meshv1.Envelope{Body: &meshv1.Envelope_Events{
			Events: &statev1.GossipEventBatch{Events: batch},
		}}
		for _, peerID := range peerIDs {
			if peerID == n.store.LocalID {
				continue
			}
			if _, ok := failed[peerID]; ok {
				continue
			}
			if err := n.mesh.Send(ctx, peerID, env); err != nil {
				n.log.Debugw("event gossip send failed", "peer", peerID.Short(), "err", err)
				failed[peerID] = struct{}{}
			}
		}
	}
}

func (n *Node) gossip(ctx context.Context) {
	digest := n.store.Clock()
	peers := n.GetConnectedPeers()
	var wg sync.WaitGroup
	for _, peerID := range peers {
		if peerID == n.store.LocalID {
			continue
		}
		wg.Go(func() {
			gossipCtx, cancel := context.WithTimeout(ctx, gossipStreamTimeout)
			defer cancel()
			if err := n.sendClockViaStream(gossipCtx, peerID, digest); err != nil {
				n.log.Debugw("clock gossip send failed", "peer", peerID.Short(), "err", err)
			}
		})
	}
	wg.Wait()
}

func (n *Node) acceptClockStreamsLoop(ctx context.Context) {
	for {
		peerKey, stream, err := n.mesh.AcceptClockStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			n.log.Debugw("accept clock stream failed", "err", err)
			return
		}
		go n.handleClockStream(ctx, peerKey, stream)
	}
}

func (n *Node) handleClockStream(_ context.Context, from types.PeerKey, stream io.ReadWriteCloser) {
	defer stream.Close()
	span := n.tracer.Start("gossip.handleClock")
	span.SetAttr("peer", from.Short())
	defer span.End()

	const maxClockSize = 256 << 10 // 256 KB
	b, err := io.ReadAll(io.LimitReader(stream, maxClockSize+1))
	if err != nil {
		n.log.Debugw("read clock stream failed", "peer", from.Short(), "err", err)
		return
	}
	if len(b) > maxClockSize {
		n.log.Warnw("clock stream exceeded size limit", "peer", from.Short(), "size", len(b))
		return
	}

	digest := &statev1.GossipStateDigest{}
	if err := digest.UnmarshalVT(b); err != nil {
		n.log.Debugw("unmarshal clock stream failed", "peer", from.Short(), "err", err)
		return
	}

	events := n.store.MissingFor(digest)
	if len(events) == 0 {
		return
	}

	batch := &statev1.GossipEventBatch{Events: events}
	resp, err := batch.MarshalVT()
	if err != nil {
		n.log.Debugw("marshal clock response failed", "peer", from.Short(), "err", err)
		return
	}
	if _, err := stream.Write(resp); err != nil {
		n.log.Debugw("write clock response failed", "peer", from.Short(), "err", err)
	}
}

func (n *Node) sendClockViaStream(ctx context.Context, peerID types.PeerKey, digest *statev1.GossipStateDigest) error {
	stream, err := n.mesh.OpenClockStream(ctx, peerID)
	if err != nil {
		return fmt.Errorf("open clock stream to %s: %w", peerID.Short(), err)
	}

	b, err := digest.MarshalVT()
	if err != nil {
		stream.Close()
		return fmt.Errorf("marshal digest: %w", err)
	}
	if _, err := stream.Write(b); err != nil {
		stream.Close()
		return fmt.Errorf("write clock to %s: %w", peerID.Short(), err)
	}

	// Half-close the write side so the responder sees EOF.
	if err := stream.CloseWrite(); err != nil {
		return fmt.Errorf("half-close clock stream to %s: %w", peerID.Short(), err)
	}

	resp, err := io.ReadAll(io.LimitReader(stream, maxResponseSize+1))
	if err != nil {
		return fmt.Errorf("read clock response from %s: %w", peerID.Short(), err)
	}
	if len(resp) == 0 {
		return nil
	}
	if len(resp) > maxResponseSize {
		return fmt.Errorf("clock response from %s exceeded size limit (%d bytes)", peerID.Short(), len(resp))
	}

	batch := &statev1.GossipEventBatch{}
	if err := batch.UnmarshalVT(resp); err != nil {
		return fmt.Errorf("unmarshal clock response from %s: %w", peerID.Short(), err)
	}

	result := n.store.ApplyEvents(batch.GetEvents(), true)
	n.queueGossipEvents(result.Rebroadcast)
	return nil
}

// eventWireSize returns the on-wire size of a single event inside a
// GossipEventBatch. Uses proto's own size calculation to avoid fragile
// hand-computed varint arithmetic.
func eventWireSize(event *statev1.GossipEvent) int {
	single := &statev1.GossipEventBatch{Events: []*statev1.GossipEvent{event}}
	empty := &statev1.GossipEventBatch{}
	return single.SizeVT() - empty.SizeVT()
}

// batchEvents packs events into groups that each fit within maxSize when
// serialized as an Envelope. Events that individually exceed maxSize are
// placed in their own batch.
func batchEvents(events []*statev1.GossipEvent, maxSize int) [][]*statev1.GossipEvent {
	if len(events) == 0 {
		return nil
	}

	// envelopeOverhead is the serialized size of an empty Envelope wrapping
	// a GossipEventBatch — covers the oneof tag, length prefixes, and batch
	// wrapper framing.
	overhead := (&meshv1.Envelope{Body: &meshv1.Envelope_Events{
		Events: &statev1.GossipEventBatch{},
	}}).SizeVT()

	var batches [][]*statev1.GossipEvent
	var current []*statev1.GossipEvent
	currentSize := overhead

	for _, event := range events {
		eventSize := eventWireSize(event)
		if len(current) > 0 && currentSize+eventSize > maxSize {
			batches = append(batches, current)
			current = nil
			currentSize = overhead
		}
		current = append(current, event)
		currentSize += eventSize
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}
