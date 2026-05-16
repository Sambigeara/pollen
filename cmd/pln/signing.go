// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
)

// signFact loads the caller's signing key from dir and invokes fn to
// produce a presigned Fact. Used by every wire-mode publish/tombstone
// path; the load-creds error message is shared across all callers.
//
// The CLI builds a one-shot presigned Fact per invocation (seq is fixed
// at 1, not a monotonic stream): wire callers do not hold the durable
// per-authority sequence the daemon's fact.Signer maintains.
func signFact(dir string, fn func(ed25519.PrivateKey) (*factv1.Fact, error)) (*factv1.Fact, error) {
	identityDir := identity.IdentityPath(dir)
	creds, err := identity.LoadCredentials(identityDir)
	if err != nil {
		return nil, fmt.Errorf("load credentials: %w", err)
	}
	if creds == nil || creds.Grant() == nil {
		return nil, errors.New("no credentials in this context; run `pln join` first")
	}
	priv, _, err := identity.EnsureIdentityKey(identityDir)
	if err != nil {
		return nil, fmt.Errorf("load identity key: %w", err)
	}
	return fn(priv)
}

// The presigned* builders below mirror, exactly, the (resource, body)
// pairing the daemon recomputes for the same spec kind in
// pkg/state/mutations.go (seedResourceID/staticResourceID/blobResourceID
// + wrapSpecBody) and re-derives at admission in gate.decodeSpecChange.
// Any divergence makes the body hash or resource mismatch and the
// daemon silently rejects the Fact, so they must stay in lock-step.

func presignedWorkload(priv ed25519.PrivateKey, hashBytes []byte, body *statev1.WorkloadSpecChange, policy *admissionv1.Predicate, deleted bool) (*factv1.Fact, error) {
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
		Name: body.GetName(),
		Hash: hashBytes,
	}}}
	return fact.IssueFact(priv, resource, body, policy, 1, deleted)
}

func presignedStatic(priv ed25519.PrivateKey, name string, manifestDigest []byte, deleted bool) (*factv1.Fact, error) {
	body := &statev1.StaticSpecChange{Name: name, ManifestDigest: manifestDigest}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{
		Name:           name,
		ManifestDigest: manifestDigest,
	}}}
	return fact.IssueFact(priv, resource, body, nil, 1, deleted)
}

func presignedBlob(priv ed25519.PrivateKey, name string, digest []byte, policy *admissionv1.Predicate, deleted bool) (*factv1.Fact, error) {
	body := &statev1.BlobSpecChange{Name: name, Digest: digest}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{
		Name:   name,
		Digest: digest,
	}}}
	return fact.IssueFact(priv, resource, body, policy, 1, deleted)
}
