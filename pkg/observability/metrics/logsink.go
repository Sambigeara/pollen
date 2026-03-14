package metrics

import "go.uber.org/zap"

// LogSink writes metric snapshots to a zap logger at Debug level.
type LogSink struct {
	log *zap.SugaredLogger
}

// NewLogSink creates a sink that writes to the given logger.
func NewLogSink(log *zap.SugaredLogger) *LogSink {
	return &LogSink{log: log}
}

func (s *LogSink) Flush(snapshots []Snapshot) {
	for _, snap := range snapshots {
		s.log.Debugw(snap.Name, "value", snap.Value)
	}
}
