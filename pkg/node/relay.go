package node

import (
	"context"
	"errors"
	"fmt"
	"sort"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/quic"
	"github.com/sambigeara/pollen/pkg/types"
)

func (n *Node) sendEnvelope(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error {
	err := n.sock.Send(ctx, peerKey, msg)
	if err == nil {
		return nil
	}
	if (!errors.Is(err, quic.ErrNoSession) && !errors.Is(err, quic.ErrNoPeer)) || msg.Type == types.MsgTypeUDPRelay {
		return err
	}

	relayPeer, ok := n.selectRelayPeer(peerKey)
	if !ok {
		return err
	}

	relayEnv := &peerv1.UdpRelayEnvelope{
		SrcPeerId: n.store.LocalID.Bytes(),
		DstPeerId: peerKey.Bytes(),
		MsgType:   uint32(msg.Type),
		Payload:   msg.Payload,
	}
	b, marshalErr := relayEnv.MarshalVT()
	if marshalErr != nil {
		return fmt.Errorf("marshal udp relay envelope: %w", marshalErr)
	}

	return n.sock.Send(ctx, relayPeer, types.Envelope{
		Type:    types.MsgTypeUDPRelay,
		Payload: b,
	})
}

// Relay trust model (v1): relays are trusted cluster members.
//
// A destination accepts src_peer_id as asserted by the relay. Relay hops still
// enforce src_peer_id == authenticated sender to reject third-party spoofing on
// the forwarding hop.
func (n *Node) handleUDPRelay(ctx context.Context, from types.PeerKey, plaintext []byte) error {
	env := &peerv1.UdpRelayEnvelope{}
	if err := env.UnmarshalVT(plaintext); err != nil {
		return fmt.Errorf("malformed udp relay envelope: %w", err)
	}

	if len(env.SrcPeerId) != len(types.PeerKey{}) || len(env.DstPeerId) != len(types.PeerKey{}) {
		return errors.New("malformed udp relay envelope: invalid peer key")
	}

	innerType := types.MsgType(env.MsgType)
	if innerType == types.MsgTypeUDPRelay {
		return errors.New("malformed udp relay envelope: nested relay")
	}

	src := types.PeerKeyFromBytes(env.SrcPeerId)
	dst := types.PeerKeyFromBytes(env.DstPeerId)
	if dst == n.store.LocalID {
		return n.handleApp(ctx, quic.Packet{
			Peer:    src,
			Typ:     innerType,
			Payload: env.Payload,
		})
	}

	if src != from {
		n.log.Debugw("dropping relayed packet with mismatched sender", "src", src.Short(), "from", from.Short(), "dst", dst.Short(), "type", innerType)
		return nil
	}

	if err := n.sock.Send(ctx, dst, types.Envelope{
		Type:    types.MsgTypeUDPRelay,
		Payload: plaintext,
	}); err != nil {
		if errors.Is(err, quic.ErrNoSession) || errors.Is(err, quic.ErrNoPeer) {
			return nil
		}
		return err
	}

	return nil
}

func (n *Node) selectRelayPeer(target types.PeerKey) (types.PeerKey, bool) {
	candidates := n.GetConnectedPeers()
	if len(candidates) == 0 {
		return types.PeerKey{}, false
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Less(candidates[j])
	})

	for _, candidate := range candidates {
		if candidate == target || candidate == n.store.LocalID {
			continue
		}
		if _, ok := n.sock.GetActivePeerAddress(candidate); !ok {
			continue
		}
		if !n.store.IsConnected(candidate, target) {
			continue
		}
		return candidate, true
	}

	return types.PeerKey{}, false
}

func (n *Node) relayReachable(peerKey types.PeerKey) bool {
	_, ok := n.selectRelayPeer(peerKey)
	return ok
}
