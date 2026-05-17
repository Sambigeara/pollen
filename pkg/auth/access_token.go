// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"buf.build/go/protovalidate"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
)

// SignAccessToken signs an ephemeral access token over a resource using
// the issuer's identity key. The issuer must be the resource's
// publisher; the gate rejects tokens whose issuer doesn't match the
// resource Fact's authority subject.
func SignAccessToken(issuerPriv ed25519.PrivateKey, resource *admissionv1.ResourceID, now time.Time, ttl time.Duration) (*admissionv1.AccessToken, error) {
	if resource == nil {
		return nil, errors.New("access token resource is required")
	}
	if err := validateResourceBody(resource); err != nil {
		return nil, err
	}
	if ttl <= 0 {
		return nil, errors.New("access token ttl must be positive")
	}
	issuerPub := issuerPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	claims := &admissionv1.AccessTokenClaims{
		Resource:      resource,
		IssuerPub:     issuerPub,
		IssuedAtUnix:  now.Unix(),
		ExpiresAtUnix: now.Add(ttl).Unix(),
	}
	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}
	sig, err := signPayload(issuerPriv, msg, sigContextAccessToken)
	if err != nil {
		return nil, err
	}
	return &admissionv1.AccessToken{Claims: claims, Signature: sig}, nil
}

// VerifyAccessToken checks the signature and expiry of token.
func VerifyAccessToken(token *admissionv1.AccessToken, now time.Time) error {
	if err := protovalidate.Validate(token); err != nil {
		return fmt.Errorf("access token invalid: %w", err)
	}
	claims := token.GetClaims()
	msg, err := signaturePayload(claims)
	if err != nil {
		return err
	}
	if err := verifyPayload(ed25519.PublicKey(claims.GetIssuerPub()), msg, token.GetSignature(), sigContextAccessToken); err != nil {
		return errors.New("access token signature invalid")
	}

	issuedAt := time.Unix(claims.GetIssuedAtUnix(), 0).Add(-timeSkewAllowance)
	expiresAt := time.Unix(claims.GetExpiresAtUnix(), 0).Add(timeSkewAllowance)
	if !expiresAt.After(issuedAt) {
		return errors.New("access token validity window invalid")
	}
	if now.Before(issuedAt) || now.After(expiresAt) {
		return errors.New("access token expired or not yet valid")
	}
	return nil
}

func EncodeAccessToken(token *admissionv1.AccessToken) (string, error) {
	b, err := token.MarshalVT()
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// validateResourceBody requires the oneof discriminator AND the inner
// variant to be populated. A discriminator-only construction (e.g.
// `&ResourceID{Body: &ResourceID_Seed{Seed: nil}}`) would otherwise
// sign a degenerate token whose Verify-side error is less clear.
func validateResourceBody(resource *admissionv1.ResourceID) error {
	switch v := resource.GetBody().(type) {
	case *admissionv1.ResourceID_Seed:
		if v.Seed == nil {
			return errors.New("access token seed body is required")
		}
	case *admissionv1.ResourceID_Service:
		if v.Service == nil {
			return errors.New("access token service body is required")
		}
	case *admissionv1.ResourceID_Static:
		if v.Static == nil {
			return errors.New("access token static body is required")
		}
	case *admissionv1.ResourceID_Blob:
		if v.Blob == nil {
			return errors.New("access token blob body is required")
		}
	case nil:
		return errors.New("access token resource body discriminator is unset")
	default:
		return fmt.Errorf("access token resource body type %T unsupported", v)
	}
	return nil
}

func DecodeAccessToken(s string) (*admissionv1.AccessToken, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	token := &admissionv1.AccessToken{}
	if err := token.UnmarshalVT(b); err != nil {
		return nil, err
	}
	return token, nil
}
