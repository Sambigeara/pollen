// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

func TestBuildShareResource_RefusesNonPublisherWorkload(t *testing.T) {
	mine := []byte{0xaa, 0x01}
	theirs := []byte{0xbb, 0x02}
	wl := &controlv1.WorkloadSummary{
		Hash:      strings.Repeat("a", 64),
		Name:      "echo",
		Publisher: &controlv1.NodeRef{PeerPub: theirs},
	}
	_, _, err := buildShareResource(wl, false, "", nil, mine)
	require.ErrorContains(t, err, "another peer")
}

func TestBuildShareResource_RefusesNonPublisherBlob(t *testing.T) {
	mine := []byte{0xaa, 0x01}
	theirs := []byte{0xbb, 0x02}
	blobs := []*controlv1.BlobSummary{
		{Hash: strings.Repeat("b", 64), Name: "payload", Publisher: &controlv1.NodeRef{PeerPub: theirs}},
	}
	_, _, err := buildShareResource(nil, true, blobs[0].GetHash(), blobs, mine)
	require.ErrorContains(t, err, "another peer")
}

func TestBuildShareResource_AllowsPublisherWorkload(t *testing.T) {
	mine := []byte{0xaa, 0x01}
	hashHex := strings.Repeat("c", 64)
	wl := &controlv1.WorkloadSummary{
		Hash:      hashHex,
		Name:      "echo",
		Publisher: &controlv1.NodeRef{PeerPub: mine},
	}
	res, sub, err := buildShareResource(wl, false, "", nil, mine)
	require.NoError(t, err)
	require.Equal(t, shareSubdomainWorkload, sub)
	want, _ := hex.DecodeString(hashHex)
	require.Equal(t, want, res.GetSeed().GetHash())
}

func TestBuildShareResource_AllowsBlobWithoutPublisher(t *testing.T) {
	// Anonymous publishers (legacy entries) leave Publisher unset.
	// Permit those rather than fail-closed: the share will still 403
	// at the gateway.
	mine := []byte{0xaa, 0x01}
	hashHex := strings.Repeat("d", 64)
	blobs := []*controlv1.BlobSummary{{Hash: hashHex, Name: "anon"}}
	res, sub, err := buildShareResource(nil, true, hashHex, blobs, mine)
	require.NoError(t, err)
	require.Equal(t, shareSubdomainBlob, sub)
	require.Equal(t, "anon", res.GetBlob().GetName())
}
