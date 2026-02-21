package logging

import (
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Init() {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	cfg.DisableStacktrace = true
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel) // TODO(saml) make configurable

	logger, err := cfg.Build()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
		panic(err)
	}

	zap.ReplaceGlobals(logger.Named("pollen"))
}
