package traces

import (
	"crypto/rand"
	"encoding/hex"
)

// TraceID is a 16-byte unique identifier for a distributed trace.
type TraceID [16]byte

// NewTraceID generates a random TraceID.
func NewTraceID() TraceID {
	var id TraceID
	_, _ = rand.Read(id[:])
	return id
}

// TraceIDFromBytes creates a TraceID from a byte slice. Returns a zero TraceID
// if the slice is not exactly 16 bytes.
func TraceIDFromBytes(b []byte) TraceID {
	var id TraceID
	if len(b) == len(id) {
		copy(id[:], b)
	}
	return id
}

// IsZero reports whether the TraceID is the zero value.
func (id TraceID) IsZero() bool { return id == TraceID{} }

// String returns the hex-encoded representation.
func (id TraceID) String() string { return hex.EncodeToString(id[:]) }

// Bytes returns the raw bytes for embedding in protobuf messages.
func (id TraceID) Bytes() []byte { return id[:] }

// SpanID is an 8-byte unique identifier for a single span within a trace.
type SpanID [8]byte

// NewSpanID generates a random SpanID.
func NewSpanID() SpanID {
	var id SpanID
	_, _ = rand.Read(id[:])
	return id
}

// IsZero reports whether the SpanID is the zero value.
func (id SpanID) IsZero() bool { return id == SpanID{} }

// String returns the hex-encoded representation.
func (id SpanID) String() string { return hex.EncodeToString(id[:]) }
