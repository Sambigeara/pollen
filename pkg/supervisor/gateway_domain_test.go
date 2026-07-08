// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalGatewayDomain(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},
		{".staging.pln.sh", ".staging.pln.sh"},
		{"staging.pln.sh", ".staging.pln.sh"},
		{".Staging.PLN.sh", ".staging.pln.sh"},
		{"EXAMPLE.com", ".example.com"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, canonicalGatewayDomain(tc.in), tc.in)
	}
}
