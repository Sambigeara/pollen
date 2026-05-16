// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

// RPCCaller is the authenticated principal behind a control RPC.
// Handlers read it via RPCCallerFromContext to make scoping and
// ownership decisions against the caller's grant.
type RPCCaller struct {
	grant *identityv1.Grant
}

func NewRPCCaller(grant *identityv1.Grant) RPCCaller {
	return RPCCaller{grant: grant}
}

func (c RPCCaller) Grant() *identityv1.Grant { return c.grant }

func (c RPCCaller) SubjectPub() types.PeerKey {
	return types.PeerKeyFromBytes(c.grant.GetClaims().GetSubjectPub())
}

func (c RPCCaller) CanAdmit() bool {
	return c.grant.GetClaims().GetCapabilities().GetCanAdmit()
}

// CanPublish reports whether the caller's grant permits publishing any
// resource kind. Per-kind enforcement lives in the admission pipeline;
// this coarse check gates the control RPCs that predate it.
func (c RPCCaller) CanPublish() bool {
	p := c.grant.GetClaims().GetCapabilities().GetPublish()
	return p.GetFunctions() || p.GetBlobs() || p.GetSites() || p.GetServices()
}

func (c RPCCaller) CanDelegate() bool {
	return c.grant.GetClaims().GetCapabilities().GetCanDelegate()
}

type rpcCallerKey struct{}

func WithRPCCaller(ctx context.Context, caller RPCCaller) context.Context {
	return context.WithValue(ctx, rpcCallerKey{}, caller)
}

func RPCCallerFromContext(ctx context.Context) (RPCCaller, bool) {
	c, ok := ctx.Value(rpcCallerKey{}).(RPCCaller)
	return c, ok
}
