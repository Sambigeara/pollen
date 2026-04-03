package metrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCollectSnapshot(t *testing.T) {
	p := NewProvider()
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	nm := NewNodeMetrics(p.Meter())
	ctx := context.Background()

	nm.CertRenewals.Add(ctx, 3)
	nm.CertRenewalsFailed.Add(ctx, 1)
	nm.PunchAttempts.Add(ctx, 10)
	nm.PunchFailures.Add(ctx, 2)
	nm.CertExpirySeconds.Record(ctx, 3600.0)

	snap, err := p.CollectSnapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), snap.CertRenewals)
	require.Equal(t, int64(1), snap.CertRenewalsFailed)
	require.Equal(t, int64(10), snap.PunchAttempts)
	require.Equal(t, int64(2), snap.PunchFailures)
	require.Equal(t, 3600.0, snap.CertExpirySeconds)
}

func TestCollectSnapshot_Noop(t *testing.T) {
	p := NewNoopProvider()
	snap, err := p.CollectSnapshot(context.Background())
	require.NoError(t, err)
	require.Zero(t, snap)
}

func TestPeerMetrics_EnabledReflectsProvider(t *testing.T) {
	p := NewProvider()
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	pm := NewPeerMetrics(p.Meter())
	require.True(t, pm.Enabled(context.Background()))
}

func TestPeerMetrics_NoopDisabled(t *testing.T) {
	pm := NewPeerMetrics(NewNoopProvider().Meter())
	require.False(t, pm.Enabled(context.Background()))
}
