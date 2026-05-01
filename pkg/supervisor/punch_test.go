// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// rankCoordinators picks who should mediate a punch attempt between local and
// target. The test fixtures model the two regimes the function distinguishes:
// cross-NAT (public relays preferred, same-subnet/same-egress excluded) and
// shared-egress (LAN-adjacent coordinators required so the punch trigger
// carries a LAN candidate instead of a hairpin-bound WAN one).

func reachableTo(targets ...types.PeerKey) map[types.PeerKey]struct{} {
	m := make(map[types.PeerKey]struct{}, len(targets))
	for _, t := range targets {
		m[t] = struct{}{}
	}
	return m
}

func TestRankCoordinatorsCrossNATPrefersPublicRelay(t *testing.T) {
	local := testPeerKey(1)
	target := testPeerKey(2)
	publicRelay := testPeerKey(3)
	sameSubnetPeer := testPeerKey(4)
	sameEgressPeer := testPeerKey(5)

	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local:  {IPs: []string{"192.168.0.10"}, ObservedExternalIP: "203.0.113.1"},
			target: {IPs: []string{"10.99.0.20"}, ObservedExternalIP: "198.51.100.7"},
			publicRelay: {
				IPs:                []string{"91.99.170.199"},
				ObservedExternalIP: "91.99.170.199",
				PubliclyAccessible: true,
				Reachable:          reachableTo(target, local),
			},
			sameSubnetPeer: {
				IPs:                []string{"192.168.0.24"},
				ObservedExternalIP: "203.0.113.1",
				Reachable:          reachableTo(target, local),
			},
			sameEgressPeer: {
				IPs:                []string{"172.16.0.5"},
				ObservedExternalIP: "203.0.113.1",
				Reachable:          reachableTo(target, local),
			},
		},
	}

	got := rankCoordinators(snap.Nodes[local].IPs, snap.Nodes[target].IPs, target,
		[]types.PeerKey{publicRelay, sameSubnetPeer, sameEgressPeer}, snap)

	require.Equal(t, []types.PeerKey{publicRelay}, got)
}

func TestRankCoordinatorsSharedEgressFallsBackToPublicRelay(t *testing.T) {
	local := testPeerKey(1)
	target := testPeerKey(2)
	publicRelay := testPeerKey(3)

	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local:  {IPs: []string{"192.168.0.31"}, ObservedExternalIP: "81.108.176.99"},
			target: {IPs: []string{"192.168.0.220"}, ObservedExternalIP: "81.108.176.99"},
			publicRelay: {
				IPs:                []string{"91.99.170.199"},
				ObservedExternalIP: "91.99.170.199",
				PubliclyAccessible: true,
				Reachable:          reachableTo(target, local),
			},
		},
	}

	got := rankCoordinators(snap.Nodes[local].IPs, snap.Nodes[target].IPs, target,
		[]types.PeerKey{publicRelay}, snap)

	require.Equal(t, []types.PeerKey{publicRelay}, got, "with no LAN-adjacent coordinator the public relay is used as hairpin fallback — punch may fail but the FSM should still try")
}

func TestRankCoordinatorsSharedEgressLANBeatsPublicFallback(t *testing.T) {
	local := testPeerKey(1)
	target := testPeerKey(2)
	lanCoord := testPeerKey(3)
	publicRelay := testPeerKey(4)

	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local:  {IPs: []string{"192.168.0.31"}, ObservedExternalIP: "81.108.176.99"},
			target: {IPs: []string{"192.168.0.220"}, ObservedExternalIP: "81.108.176.99"},
			lanCoord: {
				IPs:                []string{"192.168.0.24"},
				ObservedExternalIP: "81.108.176.99",
				Reachable:          reachableTo(target, local),
			},
			publicRelay: {
				IPs:                []string{"91.99.170.199"},
				ObservedExternalIP: "91.99.170.199",
				PubliclyAccessible: true,
				Reachable:          reachableTo(target, local),
			},
		},
	}

	got := rankCoordinators(snap.Nodes[local].IPs, snap.Nodes[target].IPs, target,
		[]types.PeerKey{publicRelay, lanCoord}, snap)

	require.Equal(t, []types.PeerKey{lanCoord, publicRelay}, got, "LAN-adjacent ranks ahead of the hairpin fallback")
}

func TestRankCoordinatorsPrefersEasyNATOverUnknown(t *testing.T) {
	local := testPeerKey(1)
	target := testPeerKey(2)
	// Order PeerKeys so the OLD (untiered) sort would give the wrong answer:
	// unknownPeer < easyNATPeer by Compare, so without tier promotion the
	// unknown peer would come first.
	unknownPeer := testPeerKey(3)
	easyNATPeer := testPeerKey(4)

	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local:  {IPs: []string{"192.168.0.10"}, ObservedExternalIP: "203.0.113.1"},
			target: {IPs: []string{"10.99.0.20"}, ObservedExternalIP: "198.51.100.7"},
			unknownPeer: {
				IPs:                []string{"203.0.113.50"},
				ObservedExternalIP: "203.0.113.50",
				Reachable:          reachableTo(target, local),
			},
			easyNATPeer: {
				IPs:                []string{"203.0.113.51"},
				ObservedExternalIP: "203.0.113.51",
				NatType:            nat.Easy,
				Reachable:          reachableTo(target, local),
			},
		},
	}

	got := rankCoordinators(snap.Nodes[local].IPs, snap.Nodes[target].IPs, target,
		[]types.PeerKey{unknownPeer, easyNATPeer}, snap)

	require.Equal(t, []types.PeerKey{easyNATPeer, unknownPeer}, got, "inferred easy-NAT outranks unknown-NAT when no admin-public relay is available")
}

func TestRankCoordinatorsAdminPublicBeatsEasyNAT(t *testing.T) {
	local := testPeerKey(1)
	target := testPeerKey(2)
	// publicRelay sorts after easyNATPeer by Compare, so without tiering the
	// easy-NAT peer would come first. Tiering must keep admin-public ahead.
	easyNATPeer := testPeerKey(3)
	publicRelay := testPeerKey(4)

	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local:  {IPs: []string{"192.168.0.10"}, ObservedExternalIP: "203.0.113.1"},
			target: {IPs: []string{"10.99.0.20"}, ObservedExternalIP: "198.51.100.7"},
			easyNATPeer: {
				IPs:                []string{"203.0.113.50"},
				ObservedExternalIP: "203.0.113.50",
				NatType:            nat.Easy,
				Reachable:          reachableTo(target, local),
			},
			publicRelay: {
				IPs:                []string{"91.99.170.199"},
				ObservedExternalIP: "91.99.170.199",
				PubliclyAccessible: true,
				Reachable:          reachableTo(target, local),
			},
		},
	}

	got := rankCoordinators(snap.Nodes[local].IPs, snap.Nodes[target].IPs, target,
		[]types.PeerKey{easyNATPeer, publicRelay}, snap)

	require.Equal(t, []types.PeerKey{publicRelay, easyNATPeer}, got, "admin-public stays the top tier even when an easy-NAT candidate is also available")
}

func TestRankCoordinatorsExcludesUnreachableCandidate(t *testing.T) {
	local := testPeerKey(1)
	target := testPeerKey(2)
	publicRelayUnreachable := testPeerKey(3)
	publicRelayOK := testPeerKey(4)

	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local:                  {IPs: []string{"192.168.0.10"}, ObservedExternalIP: "203.0.113.1"},
			target:                 {IPs: []string{"10.99.0.20"}, ObservedExternalIP: "198.51.100.7"},
			publicRelayUnreachable: {IPs: []string{"203.0.113.50"}, ObservedExternalIP: "203.0.113.50", PubliclyAccessible: true, Reachable: reachableTo(local)},
			publicRelayOK:          {IPs: []string{"203.0.113.51"}, ObservedExternalIP: "203.0.113.51", PubliclyAccessible: true, Reachable: reachableTo(target, local)},
		},
	}

	got := rankCoordinators(snap.Nodes[local].IPs, snap.Nodes[target].IPs, target,
		[]types.PeerKey{publicRelayUnreachable, publicRelayOK}, snap)

	require.Equal(t, []types.PeerKey{publicRelayOK}, got)
}
