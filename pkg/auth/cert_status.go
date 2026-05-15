// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"bytes"
	"fmt"
	"slices"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
)

// DefaultAccessDeadlineTTL is the hard ceiling applied at delegated cert
// mint sites where the issuer has not specified one. Bounds how long a
// caller can stay in the cluster on the same root-of-trust before a
// re-bootstrap (fresh join token) is required.
const DefaultAccessDeadlineTTL = 30 * 24 * time.Hour

// CertStatus is the typed result of a cert validity check.
//
// Two distinct expiry states matter for the renewal model:
// NeedsRenewal (cert past not_after but within access_deadline, can
// still mint a successor via the renewal endpoint) and Expired (cert
// past access_deadline, re-join required).
type CertStatus int

const (
	CertStatusOK CertStatus = iota
	CertStatusNotYetValid
	CertStatusNeedsRenewal
	CertStatusExpired
	CertStatusRevoked
	CertStatusInvalidChain
	CertStatusSubjectMismatch
)

func (s CertStatus) String() string {
	switch s {
	case CertStatusOK:
		return "ok"
	case CertStatusNotYetValid:
		return "not-yet-valid"
	case CertStatusNeedsRenewal:
		return "needs-renewal"
	case CertStatusExpired:
		return "expired"
	case CertStatusRevoked:
		return "revoked"
	case CertStatusInvalidChain:
		return "invalid-chain"
	case CertStatusSubjectMismatch:
		return "subject-mismatch"
	}
	return fmt.Sprintf("unknown(%d)", int(s))
}

// CanAuthenticate reports whether a cert in this status may be presented
// to open authenticated mTLS sessions. Past not_after, false: a fresh
// cert must be obtained via the renewal endpoint first.
func (s CertStatus) CanAuthenticate() bool {
	return s == CertStatusOK
}

// CanRenew reports whether a cert in this status may be presented to
// the renewal endpoint to mint a successor. True while the chain is
// structurally sound and access_deadline (the hard ceiling) hasn't
// passed.
func (s CertStatus) CanRenew() bool {
	return s == CertStatusOK || s == CertStatusNeedsRenewal
}

// DenyChecker reports whether a subject pubkey is currently denied.
// Implementations typically wrap a cluster snapshot.
type DenyChecker func(subjectPub []byte) bool

// CertCheck is the typed result of CheckCert. Reason is human-readable
// detail suitable for CLI error surfacing; the time fields are always
// populated from cert claims even on failure paths.
type CertCheck struct {
	NotBefore      time.Time
	NotAfter       time.Time
	AccessDeadline time.Time
	Reason         string
	Status         CertStatus
}

// CheckCert validates a delegation cert and returns its typed status.
// Performs chain + signature verification, optional subject-match,
// time-window check, and optional denylist consultation, in that order.
//
// expectedSubject = nil skips subject matching; denied = nil skips the
// denylist check. The denylist check is chain-aware: the cert is
// rejected if its leaf subject OR any subject in its delegation chain
// is denied, so revoking an admin poisons its whole subtree.
// access_deadline = 0 in the cert is treated as "no hard ceiling"
// (equivalent to access_deadline = not_after). Certs minted before
// access_deadline was populated continue to validate.
func CheckCert(
	cert *admissionv1.DelegationCert,
	rootPub []byte,
	now time.Time,
	expectedSubject []byte,
	denied DenyChecker,
) CertCheck {
	if cert == nil {
		return CertCheck{Status: CertStatusInvalidChain, Reason: "cert is nil"}
	}
	claims := cert.GetClaims()
	nb := time.Unix(claims.GetNotBeforeUnix(), 0)
	na := time.Unix(claims.GetNotAfterUnix(), 0)
	var ad time.Time
	if dl := claims.GetAccessDeadlineUnix(); dl > 0 {
		ad = time.Unix(dl, 0)
	}
	out := CertCheck{NotBefore: nb, NotAfter: na, AccessDeadline: ad}

	got, err := verifyDelegationCertChain(cert)
	if err != nil {
		out.Status = CertStatusInvalidChain
		out.Reason = err.Error()
		return out
	}
	if !bytes.Equal(got, rootPub) {
		out.Status = CertStatusInvalidChain
		out.Reason = "chain root mismatch"
		return out
	}
	if len(expectedSubject) > 0 && !bytes.Equal(claims.GetSubjectPub(), expectedSubject) {
		out.Status = CertStatusSubjectMismatch
		out.Reason = "subject does not match expected"
		return out
	}
	if denied != nil {
		if slices.ContainsFunc(ChainSubjectPubs(cert), denied) {
			out.Status = CertStatusRevoked
			out.Reason = "subject or chain ancestor is denied"
			return out
		}
	}
	if now.Before(nb.Add(-timeSkewAllowance)) {
		out.Status = CertStatusNotYetValid
		out.Reason = fmt.Sprintf("not yet valid until %s", nb.UTC().Format(time.RFC3339))
		return out
	}
	if !ad.IsZero() && now.After(ad.Add(timeSkewAllowance)) {
		out.Status = CertStatusExpired
		out.Reason = fmt.Sprintf("access deadline passed at %s; re-join required", ad.UTC().Format(time.RFC3339))
		return out
	}
	if now.After(na.Add(timeSkewAllowance)) {
		// Legacy certs without an access_deadline have no renewal window;
		// past not_after they are fully expired.
		if ad.IsZero() {
			out.Status = CertStatusExpired
			out.Reason = fmt.Sprintf("expired at %s", na.UTC().Format(time.RFC3339))
			return out
		}
		out.Status = CertStatusNeedsRenewal
		out.Reason = fmt.Sprintf("not_after passed at %s; renewable until %s", na.UTC().Format(time.RFC3339), ad.UTC().Format(time.RFC3339))
		return out
	}
	out.Status = CertStatusOK
	return out
}
