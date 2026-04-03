package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New creates a configured zap logger for console output.
func New() (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	cfg.DisableStacktrace = true
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel) // TODO(saml): make configurable

	return cfg.Build()
}
