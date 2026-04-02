package transport_test

import (
	"net/netip"
	"testing"

	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

var _ = transport.PeerEvent{
	Key:   types.PeerKey{},
	Type:  transport.PeerEventConnected,
	Addrs: []netip.AddrPort{},
}

func TestPeerEventTypeConstants(t *testing.T) {
	require.Equal(t, transport.PeerEventType(0), transport.PeerEventConnected)
	require.Equal(t, transport.PeerEventType(1), transport.PeerEventDisconnected)
}

func TestStreamTypeConstantsExported(t *testing.T) {
	require.Equal(t, transport.StreamType(1), transport.StreamTypeDigest)
	require.Equal(t, transport.StreamType(2), transport.StreamTypeTunnel)
	require.Equal(t, transport.StreamType(3), transport.StreamTypeRouted)
	require.Equal(t, transport.StreamType(4), transport.StreamTypeArtifact)
	require.Equal(t, transport.StreamType(5), transport.StreamTypeWorkload)
}
