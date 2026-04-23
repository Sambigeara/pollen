// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/claims"
	"google.golang.org/protobuf/types/known/structpb"
)

func claimToProto(c *claims.PublisherClaim) *statev1.PublisherClaim {
	if c == nil {
		return nil
	}
	var props *structpb.Struct
	if len(c.Properties) > 0 {
		if p, err := structpb.NewStruct(c.Properties); err == nil {
			props = p
		}
	}
	return &statev1.PublisherClaim{Properties: props, Signature: c.Signature}
}

func claimFromProto(p *statev1.PublisherClaim) *claims.PublisherClaim {
	if p == nil {
		return nil
	}
	var props map[string]any
	if p.GetProperties() != nil {
		props = p.GetProperties().AsMap()
	}
	return claims.New(props, p.GetSignature())
}
