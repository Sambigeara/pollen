// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
)

// signWith loads the caller's NodeCredentials and identity key from dir
// and invokes fn with both to produce a SpecAuth. Used by every
// wire-mode publish/tombstone path; the load-creds error message is
// shared across all callers via this helper.
func signWith(dir string, fn func(ed25519.PrivateKey, *admissionv1.DelegationCert) (*admissionv1.SpecAuth, error)) (*admissionv1.SpecAuth, error) {
	identityDir := auth.IdentityPath(dir)
	creds, err := auth.LoadNodeCredentials(identityDir)
	if err != nil {
		return nil, fmt.Errorf("load credentials: %w", err)
	}
	if creds == nil || creds.Cert() == nil {
		return nil, errors.New("no credentials in this context; run `pln join` first")
	}
	priv, _, err := auth.EnsureIdentityKey(identityDir)
	if err != nil {
		return nil, fmt.Errorf("load identity key: %w", err)
	}
	return fn(priv, creds.Cert())
}
