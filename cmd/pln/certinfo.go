// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
)

const certTimeFormat = "2 Jan 2006 15:04 MST"

// contextCertCheck loads the context's delegation cert and classifies
// it. ok is false when the context has no credentials yet.
func contextCertCheck(dir string) (*admissionv1.DelegationCert, auth.CertCheck, bool) {
	creds, err := auth.LoadNodeCredentials(auth.IdentityPath(dir))
	if err != nil || creds == nil || creds.Cert() == nil {
		return nil, auth.CertCheck{}, false
	}
	return creds.Cert(), auth.CheckCert(creds.Cert(), creds.RootPub(), time.Now(), nil, nil), true
}

// wireCertDiagnosis turns an opaque wire-mode dial failure into a
// precise, actionable message by inspecting the local context cert.
// "remote error: tls: bad certificate" almost always means the local
// cert is expired or no longer chains to the cluster root, not that the
// endpoint is down. Say which.
func wireCertDiagnosis(dir, host string, rpcErr error) string {
	base := fmt.Sprintf("cannot reach %s: %v", host, rpcErr)
	_, chk, ok := contextCertCheck(dir)
	if !ok {
		return base + "\n  no credentials in this context; run `pln join <token>` first"
	}
	switch chk.Status {
	case auth.CertStatusOK:
		return base + fmt.Sprintf("\n  context cert is valid (expires %s); the endpoint is unreachable or its server identity changed",
			chk.NotAfter.Format(certTimeFormat))
	case auth.CertStatusNeedsRenewal:
		return base + "\n  context cert is past not_after; auto-renew should have run, so the cluster may be unreachable for renewal. Retry, or rejoin if it persists"
	case auth.CertStatusExpired:
		return base + fmt.Sprintf("\n  context cert expired at %s beyond renewal; rejoin with `pln join <token>` (mint one on the cluster with `pln invite`)",
			chk.AccessDeadline.Format(certTimeFormat))
	case auth.CertStatusRevoked:
		return base + "\n  this context's identity has been denied by the cluster; you need a fresh `pln join <token>`"
	case auth.CertStatusNotYetValid:
		return base + fmt.Sprintf("\n  context cert is not valid until %s; check this machine's clock",
			chk.NotBefore.Format(certTimeFormat))
	case auth.CertStatusInvalidChain, auth.CertStatusSubjectMismatch:
		return base + "\n  context cert no longer validates against the stored root (the cluster was likely re-rooted); rejoin with a fresh `pln join <token>`"
	}
	return base
}

func runContextShow(cmd *cobra.Command, args []string) error {
	name := resolveContextName()
	if len(args) == 1 {
		name = args[0]
	}
	dir, host, err := resolveContextBindings(name, defaultRootDir())
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	fmt.Fprintf(w, "context: %s\n", name)
	if host != "" {
		fmt.Fprintf(w, "target:  %s\n", host)
	}
	if dir != "" {
		fmt.Fprintf(w, "dir:     %s\n", dir)
	}

	cert, chk, ok := contextCertCheck(dir)
	if !ok {
		fmt.Fprintln(w, "identity: none (run `pln join <token>`)")
		return nil
	}
	writeCertIdentity(w, cert, chk)
	return nil
}

func writeCertIdentity(w io.Writer, cert *admissionv1.DelegationCert, chk auth.CertCheck) {
	claims := cert.GetClaims()
	subject := types.PeerKeyFromBytes(claims.GetSubjectPub())
	issuer := types.PeerKeyFromBytes(claims.GetIssuerPub())
	caps := claims.GetCapabilities()

	fmt.Fprintf(w, "identity: %s\n", subject.Short())
	fmt.Fprintf(w, "issuer:   %s\n", issuer.Short())
	fmt.Fprintf(w, "status:   %s\n", chk.Status)
	fmt.Fprintf(w, "not before: %s\n", chk.NotBefore.Format(certTimeFormat))
	fmt.Fprintf(w, "not after:  %s\n", chk.NotAfter.Format(certTimeFormat))
	if !chk.AccessDeadline.IsZero() {
		fmt.Fprintf(w, "rejoin by:  %s\n", chk.AccessDeadline.Format(certTimeFormat))
	}
	fmt.Fprintf(w, "caps:     delegate=%t admit=%t publish=%t max_depth=%d\n",
		caps.GetCanDelegate(), caps.GetCanAdmit(), caps.GetCanPublish(), caps.GetMaxDepth())
}
