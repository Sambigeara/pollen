package membership

import (
	"context"
	"errors"
	"testing"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

func TestBroadcastGossipBatchesStopsRetryingFailedPeer(t *testing.T) {
	failPeer := peerKey(1)
	okPeer := peerKey(2)
	localID := peerKey(99)
	recorder := &sendRecorder{failPeer: failPeer, failAfter: 1, err: errors.New("boom")}

	svc := &Service{
		mesh:    recorder,
		localID: localID,
		log:     zap.S(),
	}

	batches := [][]*statev1.GossipEvent{{{}}, {{}}}
	svc.broadcastGossipBatches(context.Background(), []types.PeerKey{failPeer, okPeer, localID}, batches)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	require.Equal(t, 1, recorder.calls[failPeer])
	require.Equal(t, 2, recorder.calls[okPeer])
	_, calledLocal := recorder.calls[localID]
	require.False(t, calledLocal)
}

func TestBroadcastGossipBatchesSkipsSelf(t *testing.T) {
	localID := peerKey(1)
	recorder := &sendRecorder{}

	svc := &Service{
		mesh:    recorder,
		localID: localID,
		log:     zap.S(),
	}

	batches := [][]*statev1.GossipEvent{{{}}}
	svc.broadcastGossipBatches(context.Background(), []types.PeerKey{localID}, batches)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	require.Empty(t, recorder.calls)
}

func TestBatchEventsSingleBatch(t *testing.T) {
	events := []*statev1.GossipEvent{{}, {}}
	batches := batchEvents(events, 2000)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2)
}

func TestBatchEventsMultipleBatches(t *testing.T) {
	events := make([]*statev1.GossipEvent, 0, 50)
	for i := range 50 {
		events = append(events, &statev1.GossipEvent{
			PeerId: string(rune(i)),
		})
	}
	batches := batchEvents(events, 100)
	require.Greater(t, len(batches), 1, "should split into multiple batches with tight size limit")

	var total int
	for _, b := range batches {
		total += len(b)
	}
	require.Equal(t, 50, total, "all events should be present across batches")
}

func TestBatchEventsEmpty(t *testing.T) {
	batches := batchEvents(nil, 1000)
	require.Nil(t, batches)
}

func TestHandleDatagramEventsForwardsAndRebroadcasts(t *testing.T) {
	localID := peerKey(1)
	from := peerKey(2)

	net := newFakeNetwork()
	net.setConnectedPeers(peerKey(3))
	st := newFakeClusterState(localID)
	st.applyDeltaFn = func(_ types.PeerKey, _ []byte) ([]state.Event, []byte, error) {
		return []state.Event{state.PeerJoined{Key: from}, state.GossipApplied{}}, nil, nil
	}

	svc := &Service{
		mesh:    net,
		store:   st,
		localID: localID,
		log:     zap.S(),
		tracer:  tracenoop.NewTracerProvider().Tracer("pollen/membership"),
		events:  make(chan state.Event, eventBufSize),
	}

	batch := &statev1.GossipEventBatch{Events: []*statev1.GossipEvent{{}}}
	data, err := (&meshv1.Envelope{Body: &meshv1.Envelope_Events{
		Events: batch,
	}}).MarshalVT()
	require.NoError(t, err)

	svc.handleDatagram(context.Background(), from, data)

	// Should forward PeerJoined but filter GossipApplied.
	ev := <-svc.events
	_, ok := ev.(state.PeerJoined)
	require.True(t, ok, "expected PeerJoined, got %T", ev)

	select {
	case extra := <-svc.events:
		t.Fatalf("unexpected extra event: %T", extra)
	default:
	}
}

func TestHandleDatagramDelegatesToHandler(t *testing.T) {
	localID := peerKey(1)
	from := peerKey(2)

	var received *meshv1.Envelope

	svc := &Service{
		mesh:    newFakeNetwork(),
		store:   newFakeClusterState(localID),
		localID: localID,
		log:     zap.S(),
		tracer:  tracenoop.NewTracerProvider().Tracer("pollen/membership"),
		events:  make(chan state.Event, eventBufSize),
		datagramHandler: func(_ context.Context, _ types.PeerKey, env *meshv1.Envelope) {
			received = env
		},
	}

	// Send a punch coordination envelope — falls through to the default handler.
	data, err := (&meshv1.Envelope{Body: &meshv1.Envelope_PunchCoordRequest{
		PunchCoordRequest: &meshv1.PunchCoordRequest{},
	}}).MarshalVT()
	require.NoError(t, err)

	svc.handleDatagram(context.Background(), from, data)
	require.NotNil(t, received)
}

func TestHandleDatagramInvalidData(t *testing.T) {
	svc := &Service{
		mesh:    newFakeNetwork(),
		store:   newFakeClusterState(peerKey(1)),
		localID: peerKey(1),
		log:     zap.S(),
		tracer:  tracenoop.NewTracerProvider().Tracer("pollen/membership"),
		events:  make(chan state.Event, eventBufSize),
	}

	// Should not panic on garbage data.
	svc.handleDatagram(context.Background(), peerKey(2), []byte("garbage"))
}
