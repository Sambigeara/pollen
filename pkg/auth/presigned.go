// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"crypto/ed25519"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
)

// SignStaticSpec signs a static-site spec for wire-mode publishing.
// The returned SpecAuth carries the caller as Publisher; the daemon
// relays it without re-signing.
func SignStaticSpec(priv ed25519.PrivateKey, cert *admissionv1.DelegationCert, name string, manifestDigest []byte) (*admissionv1.SpecAuth, error) {
	body := &statev1.StaticSpecChange{Name: name, ManifestDigest: manifestDigest}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{
		Name:           name,
		ManifestDigest: manifestDigest,
	}}}
	return IssueSpecAuth(priv, cert, resource, body, nil, false)
}

// SignWorkloadSpec signs a workload spec for wire-mode publishing.
// hashBytes is the binary sha256 of the wasm binary (the same value
// encoded as hex on body.Hash).
func SignWorkloadSpec(priv ed25519.PrivateKey, cert *admissionv1.DelegationCert, hashBytes []byte, body *statev1.WorkloadSpecChange, policy *admissionv1.Predicate) (*admissionv1.SpecAuth, error) {
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
		Name: body.GetName(),
		Hash: hashBytes,
	}}}
	return IssueSpecAuth(priv, cert, resource, body, policy, false)
}

// SignBlobSpec signs a named-blob spec for wire-mode publishing.
func SignBlobSpec(priv ed25519.PrivateKey, cert *admissionv1.DelegationCert, name string, digest []byte, policy *admissionv1.Predicate) (*admissionv1.SpecAuth, error) {
	body := &statev1.BlobSpecChange{Name: name, Digest: digest}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{
		Name:   name,
		Digest: digest,
	}}}
	return IssueSpecAuth(priv, cert, resource, body, policy, false)
}

// SignWorkloadTombstone signs a deletion auth for a workload spec.
// The body must match the original publish (same fields signed at
// create time) so the validate hook accepts it on the daemon side.
func SignWorkloadTombstone(priv ed25519.PrivateKey, cert *admissionv1.DelegationCert, hashBytes []byte, body *statev1.WorkloadSpecChange, policy *admissionv1.Predicate) (*admissionv1.SpecAuth, error) {
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
		Name: body.GetName(),
		Hash: hashBytes,
	}}}
	return IssueSpecAuth(priv, cert, resource, body, policy, true)
}

// SignStaticTombstone signs a deletion auth for a static-site spec.
func SignStaticTombstone(priv ed25519.PrivateKey, cert *admissionv1.DelegationCert, name string, manifestDigest []byte) (*admissionv1.SpecAuth, error) {
	body := &statev1.StaticSpecChange{Name: name, ManifestDigest: manifestDigest}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{
		Name:           name,
		ManifestDigest: manifestDigest,
	}}}
	return IssueSpecAuth(priv, cert, resource, body, nil, true)
}

// SignBlobTombstone signs a deletion auth for a named-blob spec.
func SignBlobTombstone(priv ed25519.PrivateKey, cert *admissionv1.DelegationCert, name string, digest []byte, policy *admissionv1.Predicate) (*admissionv1.SpecAuth, error) {
	body := &statev1.BlobSpecChange{Name: name, Digest: digest}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{
		Name:   name,
		Digest: digest,
	}}}
	return IssueSpecAuth(priv, cert, resource, body, policy, true)
}
