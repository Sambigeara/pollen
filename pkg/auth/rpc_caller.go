// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

// RPCCaller is the authenticated principal behind a control RPC.
// Handlers read it via RPCCallerFromContext to make scoping and
// ownership decisions against the caller's cert.
type RPCCaller struct {
	cert *admissionv1.DelegationCert
}

func NewRPCCaller(cert *admissionv1.DelegationCert) RPCCaller {
	return RPCCaller{cert: cert}
}

func (c RPCCaller) Cert() *admissionv1.DelegationCert { return c.cert }

func (c RPCCaller) SubjectPub() types.PeerKey {
	return types.PeerKeyFromBytes(c.cert.GetClaims().GetSubjectPub())
}

func (c RPCCaller) CanAdmit() bool {
	return c.cert.GetClaims().GetCapabilities().GetCanAdmit()
}

func (c RPCCaller) CanPublish() bool {
	return c.cert.GetClaims().GetCapabilities().GetCanPublish()
}

func (c RPCCaller) CanDelegate() bool {
	return c.cert.GetClaims().GetCapabilities().GetCanDelegate()
}

type rpcCallerKey struct{}

func WithRPCCaller(ctx context.Context, caller RPCCaller) context.Context {
	return context.WithValue(ctx, rpcCallerKey{}, caller)
}

func RPCCallerFromContext(ctx context.Context) (RPCCaller, bool) {
	c, ok := ctx.Value(rpcCallerKey{}).(RPCCaller)
	return c, ok
}
