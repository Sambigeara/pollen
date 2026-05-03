// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package nat

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func hasObserver(d *Detector, ip netip.Addr) bool {
	for _, obs := range d.observations {
		if obs.ip == ip {
			return true
		}
	}
	return false
}

func TestDetectorUnknownWithOneObservation(t *testing.T) {
	d := NewDetector()
	typ, changed := d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	require.Equal(t, Unknown, typ)
	require.False(t, changed)
}

func TestDetectorUnknownWithTwoMatchingObservations(t *testing.T) {
	d := NewDetector()
	d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	typ, changed := d.AddObservation(netip.MustParseAddr("5.6.7.8"), 5000)
	require.Equal(t, Unknown, typ)
	require.False(t, changed)
}

func TestDetectorEasyWithThreeMatchingObservations(t *testing.T) {
	d := NewDetector()
	d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	d.AddObservation(netip.MustParseAddr("5.6.7.8"), 5000)
	typ, changed := d.AddObservation(netip.MustParseAddr("9.10.11.12"), 5000)
	require.Equal(t, Easy, typ)
	require.True(t, changed)
}

func TestDetectorHardWithDifferentPorts(t *testing.T) {
	d := NewDetector()
	d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	typ, changed := d.AddObservation(netip.MustParseAddr("5.6.7.8"), 6000)
	require.Equal(t, Hard, typ)
	require.True(t, changed)
}

func TestDetectorSameObserverIPCountsAsOne(t *testing.T) {
	d := NewDetector()
	d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	typ, changed := d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	require.Equal(t, Unknown, typ)
	require.False(t, changed)
}

func TestDetectorSameObserverIPDifferentPort(t *testing.T) {
	d := NewDetector()
	d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	typ, changed := d.AddObservation(netip.MustParseAddr("1.2.3.4"), 6000)
	require.Equal(t, Unknown, typ)
	require.False(t, changed)
}

func TestDetectorTransitionEasyToHard(t *testing.T) {
	d := NewDetector()
	d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	d.AddObservation(netip.MustParseAddr("5.6.7.8"), 5000)
	d.AddObservation(netip.MustParseAddr("9.10.11.12"), 5000)
	require.Equal(t, Easy, d.Type())

	typ, changed := d.AddObservation(netip.MustParseAddr("13.14.15.16"), 7000)
	require.Equal(t, Hard, typ)
	require.True(t, changed)
}

func TestDetectorObserverPortRefresh(t *testing.T) {
	d := NewDetector()
	d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	d.AddObservation(netip.MustParseAddr("5.6.7.8"), 5000)
	d.AddObservation(netip.MustParseAddr("9.10.11.12"), 5000)
	require.Equal(t, Easy, d.Type())

	typ, changed := d.AddObservation(netip.MustParseAddr("1.2.3.4"), 9000)
	require.Equal(t, Hard, typ)
	require.True(t, changed)
}

func TestDetectorResetClearsObservations(t *testing.T) {
	d := NewDetector()
	d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	d.AddObservation(netip.MustParseAddr("5.6.7.8"), 5000)
	d.AddObservation(netip.MustParseAddr("9.10.11.12"), 5000)
	require.Equal(t, Easy, d.Type())

	d.Reset()
	require.Equal(t, Unknown, d.Type())

	typ, changed := d.AddObservation(netip.MustParseAddr("99.99.99.99"), 9000)
	require.Equal(t, Unknown, typ)
	require.False(t, changed) // Unknown → Unknown (1 observation)
	require.Len(t, d.observations, 1)
}

func TestDetectorEvictsStaleObserver(t *testing.T) {
	d := NewDetector()

	for i := range maxObservations {
		ip := netip.AddrFrom4([4]byte{10, 0, byte(i >> 8), byte(i)})
		d.AddObservation(ip, 5000)
	}
	require.Equal(t, Easy, d.Type())
	require.Len(t, d.observations, maxObservations)

	typ, changed := d.AddObservation(netip.MustParseAddr("99.99.99.99"), 5000)
	require.Equal(t, Easy, typ)
	require.False(t, changed)
	require.Len(t, d.observations, maxObservations)
	require.False(t, hasObserver(d, netip.AddrFrom4([4]byte{10, 0, 0, 0})))
}

func TestDetectorEvictionSparesFreshObserver(t *testing.T) {
	d := NewDetector()

	for i := range maxObservations {
		ip := netip.AddrFrom4([4]byte{10, 0, 0, byte(i)})
		d.AddObservation(ip, 5000)
	}

	d.AddObservation(netip.AddrFrom4([4]byte{10, 0, 0, 0}), 5000)

	d.AddObservation(netip.MustParseAddr("99.99.99.99"), 5000)
	require.True(t, hasObserver(d, netip.AddrFrom4([4]byte{10, 0, 0, 0})), "re-observed entry should survive eviction")
	require.False(t, hasObserver(d, netip.AddrFrom4([4]byte{10, 0, 0, 1})), "stalest entry should be evicted")
}
