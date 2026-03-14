//go:build consolidation_target

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEnvelopeDeleted verifies that the Envelope struct no longer exists.
// This file redefines Envelope locally. If the production Envelope still
// exists, this redeclaration will cause a compile error.
type Envelope struct{ Deleted bool }

func TestEnvelopeRemoved(t *testing.T) {
	// If types.Envelope still existed, this local Envelope redeclaration
	// would fail to compile with "Envelope redeclared in this block".
	e := Envelope{Deleted: true}
	require.True(t, e.Deleted)

	// Verify the package still exports PeerKey and its methods.
	var pk PeerKey
	pk[0] = 42
	require.Equal(t, byte(42), pk.Bytes()[0])
	require.NotEmpty(t, pk.String())
	require.NotEmpty(t, pk.Short())

	// Verify PeerKeyFromBytes still works.
	b := make([]byte, 32)
	b[0] = 1
	pk2 := PeerKeyFromBytes(b)
	require.Equal(t, byte(1), pk2[0])

	// Verify Compare and Less.
	require.True(t, pk2.Less(pk))
	require.Equal(t, -1, pk2.Compare(pk))
}
