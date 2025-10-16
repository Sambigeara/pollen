package logging

import (
	"go.uber.org/zap"
)

func Init() {
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)

	l, err := cfg.Build()
	if err != nil {
		panic(err)
	}

	zap.ReplaceGlobals(l)
}
