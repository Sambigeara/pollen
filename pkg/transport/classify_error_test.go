package transport

import (
	"errors"
	"testing"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

func TestClassifyQUICErrorMapsCloseReasons(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want disconnectReason
	}{
		{
			name: "topology prune",
			err:  &quic.ApplicationError{ErrorMessage: string(closeReasonTopologyPrune)},
			want: disconnectTopologyPrune,
		},
		{
			name: "denied",
			err:  &quic.ApplicationError{ErrorMessage: string(closeReasonDenied)},
			want: disconnectDenied,
		},
		{
			name: "cert expired",
			err:  &quic.ApplicationError{ErrorMessage: string(closeReasonCertExpired)},
			want: disconnectCertExpired,
		},
		{
			name: "cert rotation",
			err:  &quic.ApplicationError{ErrorMessage: string(closeReasonCertRotation)},
			want: disconnectCertRotation,
		},
		{
			name: "duplicate",
			err:  &quic.ApplicationError{ErrorMessage: string(closeReasonDuplicate)},
			want: disconnectDuplicate,
		},
		{
			name: "shutdown",
			err:  &quic.ApplicationError{ErrorMessage: string(closeReasonShutdown)},
			want: disconnectShutdown,
		},
		{
			name: "generic graceful",
			err:  &quic.ApplicationError{ErrorMessage: "disconnect"},
			want: disconnectGraceful,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classifyQUICError(tt.err))
		})
	}
}

func TestClassifyQUICErrorIdleAndReset(t *testing.T) {
	require.Equal(t, disconnectIdleTimeout, classifyQUICError(&quic.IdleTimeoutError{}))
	require.Equal(t, disconnectReset, classifyQUICError(&quic.StatelessResetError{}))
	require.Equal(t, disconnectUnknown, classifyQUICError(errors.New("boom")))
}
