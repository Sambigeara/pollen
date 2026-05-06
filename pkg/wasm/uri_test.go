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
			name:    "service with extra path",
			input:   "pln://service/store/extra",
			wantErr: "service must not include path",
		},
		{
			name:    "service with empty extra path",
			input:   "pln://service/store/",
			wantErr: "service must not include path",
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

func TestParseTailCallMarker(t *testing.T) {
	marker := []byte(`{"kind":"tail_call","uri":"pln://seed/upper/handle","input":"aGVsbG8="}`)

	got, ok, err := ParseTailCallMarker(marker)

	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, TailCall{
		URI:   URI{Scheme: SchemeSeed, Name: "upper", Function: "handle"},
		Input: []byte("hello"),
	}, got)
}

func TestParseTailCallMarkerIgnoresNormalOutput(t *testing.T) {
	tests := [][]byte{
		[]byte("hello"),
		[]byte(`{"kind":"result","value":1}`),
		[]byte(`{"uri":"pln://seed/upper/handle","input":"aGVsbG8="}`),
	}

	for _, output := range tests {
		got, ok, err := ParseTailCallMarker(output)

		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, TailCall{}, got)
	}
}

func TestParseTailCallMarkerRejectsMalformedMarkers(t *testing.T) {
	tests := []struct {
		name   string
		output []byte
	}{
		{
			name:   "missing uri",
			output: []byte(`{"kind":"tail_call","input":"aGVsbG8="}`),
		},
		{
			name:   "invalid uri",
			output: []byte(`{"kind":"tail_call","uri":"not-a-uri","input":"aGVsbG8="}`),
		},
		{
			name:   "invalid input",
			output: []byte(`{"kind":"tail_call","uri":"pln://seed/upper/handle","input":"not base64"}`),
		},
		{
			name:   "structured input",
			output: []byte(`{"kind":"tail_call","uri":"pln://seed/upper/handle","input":{"value":1}}`),
		},
		{
			name:   "truncated marker",
			output: []byte(`{"kind":"tail_call","uri":"pln://seed/upper/handle"`),
		},
		{
			name:   "non-string kind",
			output: []byte(`{"kind":123,"uri":"pln://seed/upper/handle","input":"aGVsbG8="}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := ParseTailCallMarker(tt.output)

			require.Error(t, err)
			require.False(t, ok)
			require.Equal(t, TailCall{}, got)
		})
	}
}
