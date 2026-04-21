// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(level string) (*zap.Logger, error) {
	parsed, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}

	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	cfg.DisableStacktrace = true
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.Level = zap.NewAtomicLevelAt(parsed)

	return cfg.Build()
}

// ParseLevel maps an empty string to info so callers can pass an unset
// config field directly.
func ParseLevel(level string) (zapcore.Level, error) {
	if level == "" {
		return zapcore.InfoLevel, nil
	}
	var parsed zapcore.Level
	if err := parsed.UnmarshalText([]byte(level)); err != nil {
		return 0, fmt.Errorf("invalid log level %q: %w", level, err)
	}
	return parsed, nil
}
