// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"errors"
	"net"
	"slices"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const punchTimeout = 3 * time.Second

const (
	relayTierAdminPublic = iota
	relayTierEasyNAT
	relayTierFallback
)

func (n *Supervisor) coordinatorPeers(target types.PeerKey) []types.PeerKey {
	snap := n.store.Snapshot()
	return rankCoordinators(snap.Nodes[snap.LocalID].IPs, snap.Nodes[target].IPs, target, n.GetConnectedPeers(), snap)
}

func rankCoordinators(localIPs, targetIPs []string, target types.PeerKey, connectedPeers []types.PeerKey, snap state.Snapshot) []types.PeerKey {
	localNV := snap.Nodes[snap.LocalID]
	targetNV := snap.Nodes[target]
	// Shared egress (same WAN IP) means a public relay would produce a
	// hairpin candidate that residential routers usually can't handle.
	// Prefer a LAN-adjacent coordinator; fall back to public relay.
	sharedEgress := localNV.ObservedExternalIP != "" && membership.SameObservedEgress(localNV.ObservedExternalIP, targetNV.ObservedExternalIP)

	primary := make([]types.PeerKey, 0, len(connectedPeers))
	fallback := make([]types.PeerKey, 0, len(connectedPeers))
	for _, key := range connectedPeers {
		if key == target {
			continue
		}
		nv, ok := snap.Nodes[key]
		if !ok || len(nv.IPs) == 0 {
			continue
		}
		if _, connected := nv.Reachable[target]; !connected {
			continue
		}
		targetLAN := membership.InferPrivatelyRoutable(targetIPs, nv.IPs)
		sourceLAN := membership.InferPrivatelyRoutable(localIPs, nv.IPs)
		candSharesEgress := membership.SameObservedEgress(localNV.ObservedExternalIP, nv.ObservedExternalIP) || membership.SameObservedEgress(targetNV.ObservedExternalIP, nv.ObservedExternalIP)

		switch {
		case sharedEgress && targetLAN:
			primary = append(primary, key)
		case sharedEgress && !candSharesEgress:
			fallback = append(fallback, key)
		case !sharedEgress && !sourceLAN && !targetLAN && !candSharesEgress:
			primary = append(primary, key)
		}
	}
	relayTier := func(nv state.NodeView) int {
		switch {
		case nv.PubliclyAccessible:
			return relayTierAdminPublic
		case nv.NatType == nat.Easy:
			return relayTierEasyNAT
		default:
			return relayTierFallback
		}
	}
	byPreference := func(a, b types.PeerKey) int {
		if at, bt := relayTier(snap.Nodes[a]), relayTier(snap.Nodes[b]); at != bt {
			return at - bt
		}
		return a.Compare(b)
	}
	slices.SortFunc(primary, byPreference)
	slices.SortFunc(fallback, byPreference)
	return append(primary, fallback...)
}

func (n *Supervisor) requestPunchCoordination(target types.PeerKey) {
	coordinators := n.coordinatorPeers(target)
	if len(coordinators) == 0 {
		n.log.Debugw("no coordinators available for punch", "peer", target.Short())
		return
	}

	req := &meshv1.PunchCoordRequest{PeerPub: target.Bytes()}
	env := &meshv1.Envelope{
		Body: &meshv1.Envelope_PunchCoordRequest{PunchCoordRequest: req},
	}
	data, err := env.MarshalVT()
	if err != nil {
		n.log.Debugw("punch coord request marshal failed", zap.Error(err))
		return
	}

	coord := coordinators[0]
	if err := n.mesh.Send(context.Background(), coord, data); err != nil {
		n.log.Debugw("punch coord request send failed", "coordinator", coord.Short(), zap.Error(err))
	}
}

func (n *Supervisor) handlePunchCoordRequest(ctx context.Context, from types.PeerKey, req *meshv1.PunchCoordRequest) {
	targetKey := types.PeerKeyFromBytes(req.PeerPub)

	fromAddr, fromOk := n.mesh.GetActivePeerAddress(from)
	targetAddr, targetOk := n.mesh.GetActivePeerAddress(targetKey)
	if !fromOk || !targetOk {
		n.log.Debugw("punch coord: missing address",
			"from", from.Short(), "fromOk", fromOk,
			"target", targetKey.Short(), "targetOk", targetOk)
		return
	}

	sendTrigger := func(to types.PeerKey, peerPub []byte, selfAddr, peerAddr string) {
		triggerData, err := (&meshv1.Envelope{Body: &meshv1.Envelope_PunchCoordTrigger{PunchCoordTrigger: &meshv1.PunchCoordTrigger{
			PeerPub:  peerPub,
			SelfAddr: selfAddr,
			PeerAddr: peerAddr,
		}}}).MarshalVT()
		if err != nil {
			return
		}
		if err := n.mesh.Send(ctx, to, triggerData); err != nil {
			n.log.Debugw("punch coord trigger send failed", "to", to.Short(), zap.Error(err))
		}
	}

	n.spawn(func() {
		n.punchSem <- struct{}{}
		defer func() { <-n.punchSem }()
		sendTrigger(from, req.PeerPub, fromAddr.String(), targetAddr.String())
	})
	n.spawn(func() {
		n.punchSem <- struct{}{}
		defer func() { <-n.punchSem }()
		sendTrigger(targetKey, from.Bytes(), targetAddr.String(), fromAddr.String())
	})
}

func (n *Supervisor) handlePunchCoordTrigger(ctx context.Context, trigger *meshv1.PunchCoordTrigger) {
	peerKey := types.PeerKeyFromBytes(trigger.PeerPub)
	if n.mesh.IsPeerConnected(peerKey) {
		return
	}

	peerAddr, err := net.ResolveUDPAddr("udp", trigger.PeerAddr)
	if err != nil {
		n.log.Debugw("punch coord trigger: bad peer addr", "addr", trigger.PeerAddr, zap.Error(err))
		return
	}

	n.log.Debugw("punch coord trigger received", "peer", peerKey.Short(), "peerAddr", peerAddr.String())

	n.spawn(func() {
		n.punchSem <- struct{}{}
		defer func() { <-n.punchSem }()

		if n.mesh.IsPeerConnected(peerKey) {
			return
		}

		localNAT := n.natDetector.Type()
		n.nodeMetrics.PunchAttempts.Add(ctx, 1)

		punchCtx, cancel := context.WithTimeout(context.Background(), punchTimeout)
		defer cancel()

		err := n.mesh.Punch(punchCtx, peerKey, peerAddr, localNAT)
		if err != nil && n.mesh.IsPeerConnecting(peerKey) {
			if errors.Is(err, transport.ErrIdentityMismatch) {
				n.log.Warnw("punch failed: peer identity mismatch", "peer", peerKey.Short(), zap.Error(err))
			} else {
				n.log.Debugw("punch failed", "peer", peerKey.Short(), zap.Error(err))
			}
			n.nodeMetrics.PunchFailures.Add(ctx, 1)
			n.mesh.ConnectFailed(peerKey)
		}
	})
}
