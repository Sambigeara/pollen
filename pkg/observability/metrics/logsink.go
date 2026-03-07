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
	fields := make([]any, 0, maxLabels*2+2) //nolint:mnd
	for _, snap := range snapshots {
		fields = fields[:0]
		fields = append(fields, "value", snap.Value)
		for _, l := range snap.Labels {
			if l.Key == "" {
				break
			}
			fields = append(fields, l.Key, l.Value)
		}
		s.log.Debugw(snap.Name, fields...)
	}
}
