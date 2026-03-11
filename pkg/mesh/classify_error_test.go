package mesh

import (
	"errors"
	"testing"

	"github.com/quic-go/quic-go"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/stretchr/testify/require"
)

func TestClassifyQUICErrorMapsCloseReasons(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want peer.DisconnectReason
	}{
		{
			name: "topology prune",
			err:  &quic.ApplicationError{ErrorMessage: string(CloseReasonTopologyPrune)},
			want: peer.DisconnectTopologyPrune,
		},
		{
			name: "denied",
			err:  &quic.ApplicationError{ErrorMessage: string(CloseReasonDenied)},
			want: peer.DisconnectDenied,
		},
		{
			name: "cert expired",
			err:  &quic.ApplicationError{ErrorMessage: string(CloseReasonCertExpired)},
			want: peer.DisconnectCertExpired,
		},
		{
			name: "cert rotation",
			err:  &quic.ApplicationError{ErrorMessage: string(CloseReasonCertRotation)},
			want: peer.DisconnectCertRotation,
		},
		{
			name: "generic graceful",
			err:  &quic.ApplicationError{ErrorMessage: "disconnect"},
			want: peer.DisconnectGraceful,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classifyQUICError(tt.err))
		})
	}
}

func TestClassifyQUICErrorIdleAndReset(t *testing.T) {
	require.Equal(t, peer.DisconnectIdleTimeout, classifyQUICError(&quic.IdleTimeoutError{}))
	require.Equal(t, peer.DisconnectReset, classifyQUICError(&quic.StatelessResetError{}))
	require.Equal(t, peer.DisconnectUnknown, classifyQUICError(errors.New("boom")))
}
