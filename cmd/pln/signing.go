// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"path/filepath"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
)

// signFact loads the caller's signing key from dir and invokes fn with
// a durable per-context fact signer to produce a presigned Fact. Used
// by every publish/tombstone path that runs over the wire. Refuses to
// run when a local daemon is up for this ctx: both signers would
// advance the same FactSeqPath without coordination and could mint
// duplicate seqs under one authority key.
func signFact(dir string, fn func(*fact.Signer) (*factv1.Fact, error)) (*factv1.Fact, error) {
	identityDir := identity.IdentityPath(dir)
	if nodeSocketActive(filepath.Join(dir, socketName)) {
		return nil, errors.New("cannot sign facts while a local daemon is running for this ctx; stop it with `pln down` or omit `--wire` so the operation flows through the daemon")
	}
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
	signer, err := fact.NewDurableSigner(priv, identity.FactSeqPath(identityDir))
	if err != nil {
		return nil, fmt.Errorf("open fact signer: %w", err)
	}
	return fn(signer)
}

// The presigned* builders below mirror, exactly, the (resource, body)
// pairing the daemon recomputes for the same spec kind in
// pkg/state/mutations.go (seedResourceID/staticResourceID/blobResourceID
// + wrapSpecBody) and re-derives at admission in decodeSpecChange.
// Any divergence makes the body hash or resource mismatch and the
// daemon silently rejects the Fact, so they must stay in lock-step.

func presignedWorkload(s *fact.Signer, hashBytes []byte, body *statev1.WorkloadSpecChange, policy *admissionv1.Predicate, deleted bool) (*factv1.Fact, error) {
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
		Name: body.GetName(),
		Hash: hashBytes,
	}}}
	return s.IssueFact(resource, body, policy, deleted)
}

func presignedStatic(s *fact.Signer, name string, manifestDigest []byte, deleted bool) (*factv1.Fact, error) {
	body := &statev1.StaticSpecChange{Name: name, ManifestDigest: manifestDigest}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{
		Name:           name,
		ManifestDigest: manifestDigest,
	}}}
	return s.IssueFact(resource, body, nil, deleted)
}

func presignedBlob(s *fact.Signer, name string, digest []byte, policy *admissionv1.Predicate, deleted bool) (*factv1.Fact, error) {
	body := &statev1.BlobSpecChange{Name: name, Digest: digest}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{
		Name:   name,
		Digest: digest,
	}}}
	return s.IssueFact(resource, body, policy, deleted)
}
