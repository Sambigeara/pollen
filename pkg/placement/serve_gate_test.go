// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
)

type gateCall struct {
	peer types.PeerKey
	hash string
}

type recordingGate struct {
	mu    sync.Mutex
	calls []gateCall

	returnInfo wasm.CallerInfo
	returnErr  error
}

func (g *recordingGate) Invoke(peer types.PeerKey, hash string) (wasm.CallerInfo, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, gateCall{peer: peer, hash: hash})
	return g.returnInfo, g.returnErr
}

func (g *recordingGate) MayHost(*admissionv1.DelegationCert, *admissionv1.SpecAuth) error {
	return nil
}

func (g *recordingGate) MayPublish(*admissionv1.DelegationCert, *admissionv1.Predicate) error {
	return nil
}

func (g *recordingGate) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

func (g *recordingGate) lastCall() gateCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.calls) == 0 {
		return gateCall{}
	}
	return g.calls[len(g.calls)-1]
}

func headerBytes(t *testing.T, hash, function string) []byte {
	t.Helper()
	require.Len(t, hash, hashLen)
	require.LessOrEqual(t, len(function), maxFuncLen)

	wireCaller := []byte(`{"attributes":{"role":"wire-spoofed"}}`)
	require.LessOrEqual(t, len(wireCaller), 0xFFFF)
	buf := make([]byte, 0, callerInfoLenSize+len(wireCaller)+hashLen+1+len(function)+4)
	var lenBuf [callerInfoLenSize]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(wireCaller)))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, wireCaller...)
	buf = append(buf, []byte(hash)...)
	buf = append(buf, byte(len(function)))
	buf = append(buf, []byte(function)...)
	var inputLen [4]byte
	binary.BigEndian.PutUint32(inputLen[:], 0)
	return append(buf, inputLen[:]...)
}

func TestServeConsultsGate(t *testing.T) {
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	gated := wasm.CallerInfo{Attributes: map[string]any{"role": "cert-bound"}}
	gate := &recordingGate{returnInfo: gated}

	store := &mockStore{}
	blobs := &mockBlobs{}
	svc := New(types.PeerKey{}, store, blobs, nil, WithGate(gate))
	defer func() { _ = svc.Stop() }()

	server, client := net.Pipe()
	t.Cleanup(func() {
		server.Close()
		client.Close()
	})

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		_, _ = client.Write(headerBytes(t, hash, "run"))
		_, _ = io.Copy(io.Discard, client)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.Serve(server, types.PeerKey{})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return")
	}
	<-clientDone

	require.Equal(t, 1, gate.callCount(), "gate.Invoke should have been called")
	last := gate.lastCall()
	require.Equal(t, hash, last.hash)
}

func TestServeClosesStreamOnGateDeny(t *testing.T) {
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	gate := &recordingGate{returnErr: wasm.ErrTargetNotFound}

	store := &mockStore{}
	blobs := &mockBlobs{}
	svc := New(types.PeerKey{}, store, blobs, nil, WithGate(gate))
	defer func() { _ = svc.Stop() }()

	server, client := net.Pipe()
	t.Cleanup(func() {
		server.Close()
		client.Close()
	})

	clientDone := make(chan error, 1)
	go func() {
		_, _ = client.Write(headerBytes(t, hash, "run"))
		var sink [1]byte
		_, err := io.ReadFull(client, sink[:])
		clientDone <- err
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.Serve(server, types.PeerKey{})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return on deny")
	}

	require.GreaterOrEqual(t, gate.callCount(), 1)
	select {
	case err := <-clientDone:
		require.Error(t, err, "client should observe stream close, not data")
	case <-time.After(time.Second):
		t.Fatal("client read did not return after deny close")
	}
}

func TestCallConsultsGate(t *testing.T) {
	const seedHash = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	gate := &recordingGate{returnInfo: wasm.CallerInfo{Attributes: map[string]any{"role": "admin"}}}

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			seedHash: {Spec: state.WorkloadSpec{Hash: seedHash, Name: "myseed", MinReplicas: 1}},
		},
	}
	s := New(peerKey(1), store, &mockBlobs{}, nil, WithGate(gate))
	defer func() { _ = s.Stop() }()

	// Dispatch fails because no claimants, but the gate must fire first.
	_, _ = s.Call(context.Background(), seedHash, "run", nil)

	require.Equal(t, 1, gate.callCount(), "Call must consult gate.Invoke before dispatching")
	last := gate.lastCall()
	require.Equal(t, seedHash, last.hash)
}

func TestCallDeniesAtGate(t *testing.T) {
	const seedHash = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	gate := &recordingGate{returnErr: wasm.ErrTargetNotFound}

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			seedHash: {Spec: state.WorkloadSpec{Hash: seedHash, Name: "myseed", MinReplicas: 1}},
		},
	}
	s := New(peerKey(1), store, &mockBlobs{}, nil, WithGate(gate))
	defer func() { _ = s.Stop() }()

	_, err := s.Call(context.Background(), seedHash, "run", nil)
	require.ErrorIs(t, err, wasm.ErrTargetNotFound)
}
