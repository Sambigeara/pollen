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
	// 2 matching observations is below the Easy threshold (3) → still Unknown.
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
	// Second observation from same IP overwrites — still only 1 unique observer.
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

	// Fourth observer with different port → Hard.
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

	// Observer's NAT mapping refreshes → sees a different port.
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

	// Fresh observation after reset starts from scratch.
	typ, changed := d.AddObservation(netip.MustParseAddr("99.99.99.99"), 9000)
	require.Equal(t, Unknown, typ)
	require.False(t, changed) // Unknown → Unknown (1 observation)
	require.Len(t, d.observations, 1)
}

func TestDetectorEvictsStaleObserver(t *testing.T) {
	d := NewDetector()

	// Fill with 16 unique observers, all same port → Easy.
	for i := range maxObservations {
		ip := netip.AddrFrom4([4]byte{10, 0, byte(i >> 8), byte(i)})
		d.AddObservation(ip, 5000)
	}
	require.Equal(t, Easy, d.Type())
	require.Len(t, d.observations, maxObservations)

	// 17th observer evicts the stalest (10.0.0.0), type stays Easy.
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

	// Re-observe the first-inserted entry — refreshes its sequence.
	d.AddObservation(netip.AddrFrom4([4]byte{10, 0, 0, 0}), 5000)

	// New observer evicts 10.0.0.1 (now the stalest), not 10.0.0.0.
	d.AddObservation(netip.MustParseAddr("99.99.99.99"), 5000)
	require.True(t, hasObserver(d, netip.AddrFrom4([4]byte{10, 0, 0, 0})), "re-observed entry should survive eviction")
	require.False(t, hasObserver(d, netip.AddrFrom4([4]byte{10, 0, 0, 1})), "stalest entry should be evicted")
}
