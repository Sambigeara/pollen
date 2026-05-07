// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"net/netip"
	"slices"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

const maxBootstrapPeers = 5

// pickBootstrapPeers returns one representative peer per network the local
// node can see. A joiner only needs to reach one: an entry per network is
// enough redundancy across reachability domains, more is wire bloat.
func pickBootstrapPeers(snap state.Snapshot, now time.Time) []*controlv1.BootstrapPeerInfo {
	type candidate struct {
		addr   string
		nv     state.NodeView
		peerID types.PeerKey
		native bool
	}

	buckets := make(map[string][]candidate)
	add := func(peerID types.PeerKey, nv state.NodeView) {
		for _, ca := range candidateAddrs(nv) {
			key := classifyNetwork(ca.addr)
			if key == "" {
				continue
			}
			buckets[key] = append(buckets[key], candidate{
				peerID: peerID,
				nv:     nv,
				addr:   ca.addr,
				native: ca.native,
			})
		}
	}
	if local, ok := snap.Nodes[snap.LocalID]; ok {
		add(snap.LocalID, local)
	}
	for peerID, nv := range snap.Nodes {
		if peerID == snap.LocalID {
			continue
		}
		if nv.CertExpiry != 0 && auth.IsExpiredAt(time.Unix(nv.CertExpiry, 0), now) {
			continue
		}
		add(peerID, nv)
	}

	bucketKeys := make([]string, 0, len(buckets))
	for k := range buckets {
		bucketKeys = append(bucketKeys, k)
	}
	slices.Sort(bucketKeys)

	chosenAddrs := make(map[types.PeerKey][]string)
	chosenOrder := make([]types.PeerKey, 0, len(bucketKeys))
	for _, key := range bucketKeys {
		cands := buckets[key]
		// Native addresses outrank NAT-mapped within a bucket: a peer
		// with a directly-advertised public IP is more stable than self
		// reachable only via observed-external mapping. Self breaks ties
		// at equal native-ness so the LAN bucket prefers us.
		slices.SortFunc(cands, func(a, b candidate) int {
			if a.native != b.native {
				if a.native {
					return -1
				}
				return 1
			}
			if a.peerID == snap.LocalID && b.peerID != snap.LocalID {
				return -1
			}
			if b.peerID == snap.LocalID && a.peerID != snap.LocalID {
				return 1
			}
			if a.nv.LastEventAt.After(b.nv.LastEventAt) {
				return -1
			}
			if b.nv.LastEventAt.After(a.nv.LastEventAt) {
				return 1
			}
			return types.PeerKey.Compare(a.peerID, b.peerID)
		})
		pick := cands[0]
		if _, exists := chosenAddrs[pick.peerID]; !exists {
			chosenOrder = append(chosenOrder, pick.peerID)
		}
		chosenAddrs[pick.peerID] = append(chosenAddrs[pick.peerID], pick.addr)
	}

	out := make([]*controlv1.BootstrapPeerInfo, 0, len(chosenOrder))
	for _, peerID := range chosenOrder {
		if len(out) >= maxBootstrapPeers {
			break
		}
		addrs := chosenAddrs[peerID]
		slices.Sort(addrs)
		addrs = slices.Compact(addrs)
		out = append(out, &controlv1.BootstrapPeerInfo{
			Peer:  &controlv1.NodeRef{PeerPub: peerID.Bytes()},
			Addrs: addrs,
		})
	}
	return out
}

type candidateAddr struct {
	addr   string
	native bool
}

// candidateAddrs flattens a NodeView's reachable addresses and tags whether
// each was advertised directly (native) or derived via NAT-mapping
// (non-native). Native addresses are preferred within each network bucket.
func candidateAddrs(nv state.NodeView) []candidateAddr {
	if nv.LocalPort == 0 {
		return nil
	}
	var out []candidateAddr
	for _, ip := range nv.IPs {
		if a, ok := joinHostPort(ip, nv.LocalPort); ok {
			out = append(out, candidateAddr{addr: a, native: true})
		}
	}
	if nv.ObservedExternalIP != "" {
		port := nv.LocalPort
		if nv.ExternalPort != 0 {
			port = nv.ExternalPort
		}
		if a, ok := joinHostPort(nv.ObservedExternalIP, port); ok {
			out = append(out, candidateAddr{addr: a, native: false})
		}
	}
	return out
}

func joinHostPort(host string, port uint32) (string, bool) {
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return "", false
	}
	if addr.Is4In6() {
		addr = addr.Unmap()
	}
	return netip.AddrPortFrom(addr, uint16(port)).String(), true
}

// classifyNetwork returns a stable bucket key for an address. Addresses
// that share a key are assumed to be reachable from the same set of
// joiners (same LAN, same public internet). An empty key marks the
// address unusable for bootstrapping anyone else.
//
// NOTE(saml): all private address spaces (RFC1918 IPv4 and IPv6 ULA)
// collapse into a single "lan" bucket. This trades wire size for a few
// edge cases:
//   - Dual-stack peer: only one address family is represented in the
//     resulting bootstrap entry. A joiner with the OTHER family as its
//     only LAN reachability cannot bootstrap via that peer. Rare for
//     typical home/office LANs where both stacks coexist on the same
//     physical link.
//   - Distinct private networks bridged through the mesh (e.g. a corp
//     10/8 and a home 192.168/16 both visible to the issuer): they
//     share one bucket, only one is represented. A joiner on the
//     unrepresented LAN fails. Pollen's typical deployment is a single
//     LAN plus public, so this is a tolerable limitation.
//
// Different public address families (v4 vs v6) stay separate so a
// joiner with only one of them still has a candidate.
func classifyNetwork(hostport string) string {
	ap, err := netip.ParseAddrPort(hostport)
	if err != nil {
		return ""
	}
	addr := ap.Addr()
	if addr.Is4In6() {
		addr = addr.Unmap()
	}
	if !addr.IsValid() || addr.IsUnspecified() || addr.IsMulticast() ||
		addr.IsLoopback() || addr.IsLinkLocalUnicast() {
		return ""
	}
	if addr.IsPrivate() {
		return "lan"
	}
	if addr.Is4() {
		return "public-v4"
	}
	return "public-v6"
}
