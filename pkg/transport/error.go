package transport

import (
	"errors"

	"github.com/quic-go/quic-go"
)

func classifyQUICError(err error) DisconnectReason {
	var idleErr *quic.IdleTimeoutError
	if errors.As(err, &idleErr) {
		return disconnectIdleTimeout
	}
	var resetErr *quic.StatelessResetError
	if errors.As(err, &resetErr) {
		return disconnectReset
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.ErrorMessage {
		case "topology_prune":
			return DisconnectTopologyPrune
		case "denied":
			return DisconnectDenied
		case "cert_expired":
			return DisconnectCertExpired
		case "cert_rotation":
			return DisconnectCertRotation
		case "duplicate":
			return disconnectDuplicate
		case "shutdown":
			return disconnectShutdown
		default:
			return DisconnectGraceful
		}
	}
	return disconnectUnknown
}
