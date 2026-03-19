package supervisor

import (
	"context"
	"errors"
	"net"
	"slices"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

const punchTimeout = 3 * time.Second

func (n *Supervisor) coordinatorPeers(target types.PeerKey) []types.PeerKey {
	snap := n.store.Snapshot()
	return rankCoordinators(snap.Nodes[snap.LocalID].IPs, snap.Nodes[target].IPs, target, n.GetConnectedPeers(), snap)
}

func rankCoordinators(localIPs, targetIPs []string, target types.PeerKey, connectedPeers []types.PeerKey, snap state.Snapshot) []types.PeerKey {
	localNV := snap.Nodes[snap.LocalID]
	targetNV := snap.Nodes[target]
	out := make([]types.PeerKey, 0, len(connectedPeers))
	for _, key := range connectedPeers {
		if key == target {
			continue
		}
		nv, ok := snap.Nodes[key]
		if !ok {
			continue
		}
		if len(nv.IPs) == 0 || membership.InferPrivatelyRoutable(localIPs, nv.IPs) || membership.InferPrivatelyRoutable(targetIPs, nv.IPs) {
			continue
		}
		if _, connected := nv.Reachable[target]; !connected {
			continue
		}
		if membership.SameObservedEgress(localNV.ObservedExternalIP, nv.ObservedExternalIP) || membership.SameObservedEgress(targetNV.ObservedExternalIP, nv.ObservedExternalIP) {
			continue
		}
		out = append(out, key)
	}
	slices.SortStableFunc(out, func(a, b types.PeerKey) int {
		aPub := snap.Nodes[a].PubliclyAccessible
		bPub := snap.Nodes[b].PubliclyAccessible
		if aPub != bPub {
			if aPub {
				return -1
			}
			return 1
		}
		return a.Compare(b)
	})
	return out
}

func (n *Supervisor) requestPunchCoordination(target types.PeerKey) {
	coordinators := n.coordinatorPeers(target)
	if len(coordinators) == 0 {
		n.log.Debugw("no coordinators available for punch", "peer", target.Short())
		return
	}

	req := &meshv1.PunchCoordRequest{PeerId: target.Bytes()}
	env := &meshv1.Envelope{
		Body: &meshv1.Envelope_PunchCoordRequest{PunchCoordRequest: req},
	}
	data, err := env.MarshalVT()
	if err != nil {
		n.log.Debugw("punch coord request marshal failed", "err", err)
		return
	}

	coord := coordinators[0]
	if err := n.mesh.Send(context.Background(), coord, data); err != nil {
		n.log.Debugw("punch coord request send failed", "coordinator", coord.Short(), "err", err)
	}
}

func (n *Supervisor) handlePunchCoordRequest(ctx context.Context, from types.PeerKey, req *meshv1.PunchCoordRequest) {
	targetKey := types.PeerKeyFromBytes(req.PeerId)

	fromAddr, fromOk := n.mesh.GetActivePeerAddress(from)
	targetAddr, targetOk := n.mesh.GetActivePeerAddress(targetKey)
	if !fromOk || !targetOk {
		n.log.Debugw("punch coord: missing address",
			"from", from.Short(), "fromOk", fromOk,
			"target", targetKey.Short(), "targetOk", targetOk)
		return
	}

	sendTrigger := func(to types.PeerKey, peerId []byte, selfAddr, peerAddr string) {
		triggerData, err := (&meshv1.Envelope{Body: &meshv1.Envelope_PunchCoordTrigger{PunchCoordTrigger: &meshv1.PunchCoordTrigger{
			PeerId:   peerId,
			SelfAddr: selfAddr,
			PeerAddr: peerAddr,
		}}}).MarshalVT()
		if err != nil {
			return
		}
		if err := n.mesh.Send(ctx, to, triggerData); err != nil {
			n.log.Debugw("punch coord trigger send failed", "to", to.Short(), "err", err)
		}
	}
	n.workers.submit(ctx, func() { sendTrigger(from, req.PeerId, fromAddr.String(), targetAddr.String()) })
	n.workers.submit(ctx, func() { sendTrigger(targetKey, from.Bytes(), targetAddr.String(), fromAddr.String()) })
}

func (n *Supervisor) handlePunchCoordTrigger(ctx context.Context, trigger *meshv1.PunchCoordTrigger) {
	peerKey := types.PeerKeyFromBytes(trigger.PeerId)
	if n.meshInternal.IsPeerConnected(peerKey) {
		return
	}

	peerAddr, err := net.ResolveUDPAddr("udp", trigger.PeerAddr)
	if err != nil {
		n.log.Debugw("punch coord trigger: bad peer addr", "addr", trigger.PeerAddr, "err", err)
		return
	}

	n.log.Infow("punch coord trigger received", "peer", peerKey.Short(), "peerAddr", peerAddr.String())

	n.workers.submit(ctx, func() {
		if n.meshInternal.IsPeerConnected(peerKey) {
			return
		}

		localNAT := n.natDetector.Type()
		n.nodeMetrics.PunchAttempts.Add(ctx, 1)

		punchCtx, cancel := context.WithTimeout(context.Background(), punchTimeout)
		err := n.meshInternal.Punch(punchCtx, peerKey, peerAddr, localNAT)
		cancel()

		if err != nil && n.meshInternal.IsPeerConnecting(peerKey) {
			if errors.Is(err, transport.ErrIdentityMismatch) {
				n.log.Warnw("punch failed: peer identity mismatch", "peer", peerKey.Short(), "err", err)
			} else {
				n.log.Debugw("punch failed", "peer", peerKey.Short(), "err", err)
			}
			n.nodeMetrics.PunchFailures.Add(ctx, 1)
			n.meshInternal.ConnectFailed(peerKey)
		}
	})
}
