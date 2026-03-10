package nat

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestDetectorTypeGetter(t *testing.T) {
	d := NewDetector()
	require.Equal(t, Unknown, d.Type())

	d.AddObservation(netip.MustParseAddr("1.2.3.4"), 5000)
	d.AddObservation(netip.MustParseAddr("5.6.7.8"), 5000)
	require.Equal(t, Unknown, d.Type()) // 2 matching → still Unknown

	d.AddObservation(netip.MustParseAddr("9.10.11.12"), 5000)
	require.Equal(t, Easy, d.Type()) // 3 matching → Easy
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

func TestTypeFromUint32RoundTrip(t *testing.T) {
	for _, typ := range []Type{Unknown, Easy, Hard} {
		require.Equal(t, typ, TypeFromUint32(typ.ToUint32()))
	}
}

func TestTypeFromUint32Invalid(t *testing.T) {
	require.Equal(t, Unknown, TypeFromUint32(99))
}

func TestDetectorClearsOnMaxObservations(t *testing.T) {
	d := NewDetector()

	// Fill with 16 unique observers, all same port → Easy after the 3rd.
	for i := range maxObservations {
		ip := netip.AddrFrom4([4]byte{10, 0, byte(i >> 8), byte(i)})
		typ, _ := d.AddObservation(ip, 5000)
		if i >= minObservationsEasy-1 {
			require.Equal(t, Easy, typ, "should be Easy at observer %d", i)
		} else if i >= minObservationsHard-1 {
			require.Equal(t, Unknown, typ, "should be Unknown at observer %d (below Easy threshold)", i)
		}
	}
	require.Len(t, d.observations, maxObservations)

	// 17th observer: map clears first, then only this one entry → Unknown.
	typ, changed := d.AddObservation(netip.MustParseAddr("99.99.99.99"), 5000)
	require.Equal(t, Unknown, typ)
	require.True(t, changed)
	require.Len(t, d.observations, 1)

	// 18th + 19th observers with same port → Easy after 3rd total.
	typ, _ = d.AddObservation(netip.MustParseAddr("88.88.88.88"), 5000)
	require.Equal(t, Unknown, typ) // only 2 matching

	typ, changed = d.AddObservation(netip.MustParseAddr("77.77.77.77"), 5000)
	require.Equal(t, Easy, typ)
	require.True(t, changed)
}

func TestTypeString(t *testing.T) {
	require.Equal(t, "unknown", Unknown.String())
	require.Equal(t, "easy", Easy.String())
	require.Equal(t, "hard", Hard.String())
}
