// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wasm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    URI
		wantErr string
	}{
		{
			name:  "seed with name and function",
			input: "pln://seed/ingest/handle",
			want:  URI{Scheme: SchemeSeed, Name: "ingest", Function: "handle"},
		},
		{
			name:  "service with name",
			input: "pln://service/store",
			want:  URI{Scheme: SchemeService, Name: "store"},
		},
		{
			name:  "seed with dotted name",
			input: "pln://seed/my.workload/process",
			want:  URI{Scheme: SchemeSeed, Name: "my.workload", Function: "process"},
		},
		{
			name:  "service with hyphenated name",
			input: "pln://service/my-api",
			want:  URI{Scheme: SchemeService, Name: "my-api"},
		},
		{
			name:    "missing prefix",
			input:   "http://seed/foo/bar",
			wantErr: "missing pln:// prefix",
		},
		{
			name:    "missing name",
			input:   "pln://seed/",
			wantErr: "missing name",
		},
		{
			name:    "seed missing function",
			input:   "pln://seed/ingest",
			wantErr: "seed requires function",
		},
		{
			name:    "seed empty function",
			input:   "pln://seed/ingest/",
			wantErr: "seed requires function",
		},
		{
			name:    "unknown scheme",
			input:   "pln://unknown/foo",
			wantErr: "unknown scheme",
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: "missing pln:// prefix",
		},
		{
			name:    "bare prefix",
			input:   "pln://",
			wantErr: "missing name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseURI(tt.input)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
