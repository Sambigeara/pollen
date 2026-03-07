package node

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"maps"
	"testing"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/stretchr/testify/require"
)

func newMinimalNode(t *testing.T, bootstrapPublic bool) *Node {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	trust := auth.NewTrustBundle(adminPub)
	cert, err := auth.IssueMembershipCert(adminPriv, trust.GetClusterId(), pub, time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), config.CertTTLs{}.AdminTTL())
	require.NoError(t, err)

	creds := &auth.NodeCredentials{Trust: trust, Cert: cert}
	stateStore, err := store.Load(t.TempDir(), pub, trust, nil)
	require.NoError(t, err)

	conf := &Config{
		Port:             0,
		AdvertisedIPs:    []string{"127.0.0.1"},
		GossipInterval:   time.Second,
		PeerTickInterval: time.Second,
		TLSIdentityTTL:   config.CertTTLs{}.TLSIdentityTTL(),
		MembershipTTL:    config.CertTTLs{}.MembershipTTL(),
		BootstrapPublic:  bootstrapPublic,
	}

	n, err := New(conf, priv, creds, stateStore, peer.NewStore(nil), t.TempDir())
	require.NoError(t, err)
	return n
}

func TestBootstrapPublicSetsAccessibleImmediately(t *testing.T) {
	n := newMinimalNode(t, true)
	require.True(t, n.store.IsPubliclyAccessible(n.store.LocalID))
}

func TestBootstrapPublicFalseIsNotAccessible(t *testing.T) {
	n := newMinimalNode(t, false)
	require.False(t, n.store.IsPubliclyAccessible(n.store.LocalID))
}

func TestBatchClocksNilAndEmpty(t *testing.T) {
	require.Nil(t, batchClocks(nil, MaxDatagramPayload))

	// An empty (non-nil) clock is semantically meaningful ("I know nothing,
	// send me everything"), so it must produce exactly one batch.
	empty := &statev1.GossipVectorClock{}
	batches := batchClocks(empty, MaxDatagramPayload)
	require.Len(t, batches, 1)
	require.Empty(t, batches[0].GetCounters())
}

func TestBatchClocksSingleBatch(t *testing.T) {
	clock := &statev1.GossipVectorClock{
		Counters: map[string]uint64{"peer-a": 1, "peer-b": 2},
	}
	batches := batchClocks(clock, MaxDatagramPayload)
	require.Len(t, batches, 1)
	require.Equal(t, clock.GetCounters(), batches[0].GetCounters())
}

func TestBatchClocksLargeClockSplits(t *testing.T) {
	counters := make(map[string]uint64, 30)
	for i := range 30 {
		// 44-char key simulates a real base64-encoded peer ID.
		counters[fmt.Sprintf("peer-%039d", i)] = uint64(i + 1)
	}
	clock := &statev1.GossipVectorClock{Counters: counters}

	batches := batchClocks(clock, MaxDatagramPayload)
	require.Greater(t, len(batches), 1, "30 entries should not fit in a single datagram")

	// Every batch must serialize within the datagram limit.
	for i, batch := range batches {
		env := &meshv1.Envelope{Body: &meshv1.Envelope_Clock{Clock: batch}}
		require.LessOrEqual(t, env.SizeVT(), MaxDatagramPayload, "batch %d exceeds datagram limit", i)
	}

	// Union of all batches must equal the original.
	merged := make(map[string]uint64)
	for _, batch := range batches {
		maps.Copy(merged, batch.GetCounters())
	}
	require.Equal(t, counters, merged)
}

func TestBatchClocksSmallMaxSize(t *testing.T) {
	counters := map[string]uint64{
		"alpha":   1,
		"bravo":   2,
		"charlie": 3,
		"delta":   4,
	}
	clock := &statev1.GossipVectorClock{Counters: counters}

	// A small maxSize forces splitting into multiple batches.
	const maxSize = 30
	batches := batchClocks(clock, maxSize)
	require.Greater(t, len(batches), 1, "should split at maxSize=%d", maxSize)

	for i, batch := range batches {
		env := &meshv1.Envelope{Body: &meshv1.Envelope_Clock{Clock: batch}}
		require.LessOrEqual(t, env.SizeVT(), maxSize, "batch %d exceeds maxSize", i)
	}

	merged := make(map[string]uint64)
	for _, batch := range batches {
		maps.Copy(merged, batch.GetCounters())
	}
	require.Equal(t, counters, merged)
}

func TestBatchClocksOversizeEntry(t *testing.T) {
	// Use a tiny maxSize so even a single entry exceeds it.
	clock := &statev1.GossipVectorClock{
		Counters: map[string]uint64{"some-peer-id": 42},
	}
	batches := batchClocks(clock, 5)
	require.Empty(t, batches, "oversize single entry should be skipped, producing no batches")
}

func TestBatchClocksOversizeAfterFlush(t *testing.T) {
	// Two entries: "a" fits alone, "oversized-peer-key-that-is-very-long"
	// does not. After flushing the batch containing "a", the oversized
	// entry carried over must also be detected and skipped.
	counters := map[string]uint64{
		"a":                                    1,
		"oversized-peer-key-that-is-very-long": 2,
	}
	clock := &statev1.GossipVectorClock{Counters: counters}

	// Pick a maxSize that fits "a" alone but not the long key.
	aOnly := clockEnvelopeSize(map[string]uint64{"a": 1})
	batches := batchClocks(clock, aOnly)

	// Only "a" should survive; the long key must be skipped.
	merged := make(map[string]uint64)
	for _, batch := range batches {
		maps.Copy(merged, batch.GetCounters())
		env := &meshv1.Envelope{Body: &meshv1.Envelope_Clock{Clock: batch}}
		require.LessOrEqual(t, env.SizeVT(), aOnly, "batch exceeds maxSize")
	}
	require.Equal(t, map[string]uint64{"a": 1}, merged)
}
