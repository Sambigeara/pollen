package traces

import (
	"crypto/rand"
	"encoding/hex"
)

// traceID is a 16-byte unique identifier for a distributed trace.
type traceID [16]byte

// newTraceID generates a random traceID.
func newTraceID() traceID {
	var id traceID
	_, _ = rand.Read(id[:])
	return id
}

// traceIDFromBytes creates a traceID from a byte slice. Returns a zero traceID
// if the slice is not exactly 16 bytes.
func traceIDFromBytes(b []byte) traceID {
	var id traceID
	if len(b) == len(id) {
		copy(id[:], b)
	}
	return id
}

func (id traceID) isZero() bool { return id == traceID{} }

// String returns the hex-encoded representation.
func (id traceID) String() string { return hex.EncodeToString(id[:]) }

// spanID is an 8-byte unique identifier for a single span within a trace.
type spanID [8]byte

// newSpanID generates a random spanID.
func newSpanID() spanID {
	var id spanID
	_, _ = rand.Read(id[:])
	return id
}

// String returns the hex-encoded representation.
func (id spanID) String() string { return hex.EncodeToString(id[:]) }
