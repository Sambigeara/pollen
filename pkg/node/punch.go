package node

import (
	"context"
	"errors"
	"net"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/types"
)

func (n *Node) coordinatorPeers(target types.PeerKey) []types.PeerKey {
	routeInfo := n.store.AllRouteInfo()
	localRec := n.store.LocalRecord()
	localIPs := n.store.NodeIPs(n.store.LocalID())
	targetIPs := n.store.NodeIPs(target)
	connectedPeers := n.getConnectedPeers()
	filtered := make([]types.PeerKey, 0, len(connectedPeers))
	for _, key := range connectedPeers {
		if key == target {
			continue
		}
		filtered = append(filtered, key)
	}
	return rankCoordinators(localIPs, targetIPs, localRec.ObservedExternalIP, target, filtered, routeInfo)
}

// requestPunchCoordination sends a punch coordination request to the best
// available coordinator peer. mesh.Send is non-blocking so this is safe to
// call directly from the event loop.
func (n *Node) requestPunchCoordination(target types.PeerKey) {
	coordinators := n.coordinatorPeers(target)
	if len(coordinators) == 0 {
		n.log.Debugw("no coordinators available for punch", "peer", target.Short())
		return
	}

	req := &meshv1.PunchCoordRequest{PeerId: target.Bytes()}
	env := &meshv1.Envelope{
		Body: &meshv1.Envelope_PunchCoordRequest{PunchCoordRequest: req},
	}

	// TODO(saml) down the line, we should probably cycle through potential coordinators per request
	coord := coordinators[0]
	if err := n.mesh.Send(context.Background(), coord, env); err != nil {
		n.log.Debugw("punch coord request send failed", "coordinator", coord.Short(), "err", err)
	}
}

// handlePunchCoordRequest sends punch triggers to both sides. mesh.Send is
// non-blocking so the two sends are done sequentially without goroutines.
func (n *Node) handlePunchCoordRequest(ctx context.Context, from types.PeerKey, req *meshv1.PunchCoordRequest) {
	targetKey := types.PeerKeyFromBytes(req.PeerId)

	fromAddr, fromOk := n.mesh.GetActivePeerAddress(from)
	targetAddr, targetOk := n.mesh.GetActivePeerAddress(targetKey)
	if !fromOk || !targetOk {
		n.log.Debugw("punch coord: missing address",
			"from", from.Short(), "fromOk", fromOk,
			"target", targetKey.Short(), "targetOk", targetOk)
		return
	}

	if err := n.mesh.Send(ctx, from, &meshv1.Envelope{Body: &meshv1.Envelope_PunchCoordTrigger{PunchCoordTrigger: &meshv1.PunchCoordTrigger{
		PeerId:   req.PeerId,
		SelfAddr: fromAddr.String(),
		PeerAddr: targetAddr.String(),
	}}}); err != nil {
		n.log.Debugw("punch coord trigger send failed", "to", from.Short(), "err", err)
	}

	if err := n.mesh.Send(ctx, targetKey, &meshv1.Envelope{Body: &meshv1.Envelope_PunchCoordTrigger{PunchCoordTrigger: &meshv1.PunchCoordTrigger{
		PeerId:   from.Bytes(),
		SelfAddr: targetAddr.String(),
		PeerAddr: fromAddr.String(),
	}}}); err != nil {
		n.log.Debugw("punch coord trigger send failed", "to", targetKey.Short(), "err", err)
	}
}

func (n *Node) handlePunchCoordTrigger(trigger *meshv1.PunchCoordTrigger) {
	peerKey := types.PeerKeyFromBytes(trigger.PeerId)
	if n.peers.InState(peerKey, peer.Connected) {
		return
	}

	peerAddr, err := net.ResolveUDPAddr("udp", trigger.PeerAddr)
	if err != nil {
		n.log.Debugw("punch coord trigger: bad peer addr", "addr", trigger.PeerAddr, "err", err)
		return
	}

	n.log.Infow("punch coord trigger received", "peer", peerKey.Short(), "peerAddr", peerAddr.String())

	select {
	case n.punchCh <- punchRequest{peerKey: peerKey, ip: peerAddr.IP, port: peerAddr.Port}:
	default:
		n.log.Debugw("punch queue full, dropping", "peer", peerKey.Short())
	}
}

func (n *Node) punchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-n.punchCh:
			if n.peers.InState(req.peerKey, peer.Connected) {
				continue
			}

			localNAT := n.natDetector.Type()
			n.nodeMetrics.PunchAttempts.Inc()

			punchCtx, cancel := context.WithTimeout(context.Background(), punchTimeout)
			err := n.mesh.Punch(punchCtx, req.peerKey, &net.UDPAddr{IP: req.ip, Port: req.port}, localNAT)
			cancel()

			if err != nil && n.peers.InState(req.peerKey, peer.Connecting) {
				if errors.Is(err, mesh.ErrIdentityMismatch) {
					n.log.Warnw("punch failed: peer identity mismatch", "peer", req.peerKey.Short(), "err", err)
				} else {
					n.log.Debugw("punch failed", "peer", req.peerKey.Short(), "err", err)
				}
				n.nodeMetrics.PunchFailures.Inc()
				n.localPeerEvents <- peer.ConnectFailed{PeerKey: req.peerKey}
			}
		}
	}
}
