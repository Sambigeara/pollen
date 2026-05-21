// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
)

const certTimeFormat = "2 Jan 2006 15:04 MST"

// contextGrantCheck loads the context's grant and classifies it. ok is
// false when the context has no credentials yet.
func contextGrantCheck(dir string) (*identityv1.Grant, identity.GrantCheck, bool) {
	creds, err := identity.LoadCredentials(identity.IdentityPath(dir))
	if err != nil || creds == nil || creds.Grant() == nil {
		return nil, identity.GrantCheck{}, false
	}
	return creds.Grant(), identity.CheckGrant(creds.Grant(), creds.RootPub(), time.Now(), nil, nil), true
}

// wireCertDiagnosis turns an opaque wire dial failure into a precise,
// actionable message by inspecting the local context grant.
// "remote error: tls: bad certificate" almost always means the local
// grant is past its deadline or no longer chains to the cluster root,
// not that the endpoint is down. Say which.
func wireCertDiagnosis(dir, host string, rpcErr error) string {
	base := fmt.Sprintf("cannot reach %s: %v", host, rpcErr)
	_, chk, ok := contextGrantCheck(dir)
	if !ok {
		return base + "\n  no credentials in this context; run `pln join <token>` first"
	}
	switch chk.Status {
	case identity.GrantStatusOK:
		if chk.GrantDeadline.IsZero() {
			return base + "\n  context grant is valid; the endpoint is unreachable or its server identity changed"
		}
		return base + fmt.Sprintf("\n  context grant is valid (rejoin by %s); the endpoint is unreachable or its server identity changed",
			chk.GrantDeadline.Format(certTimeFormat))
	case identity.GrantStatusExpired:
		return base + fmt.Sprintf("\n  context grant deadline passed at %s; rejoin with `pln join <token>` (mint one on the cluster with `pln invite`)",
			chk.GrantDeadline.Format(certTimeFormat))
	case identity.GrantStatusRevoked:
		return base + "\n  this context's identity has been denied by the cluster; you need a fresh `pln join <token>`"
	case identity.GrantStatusNotYetValid:
		return base + fmt.Sprintf("\n  context grant is not valid until %s; check this machine's clock",
			chk.NotBefore.Format(certTimeFormat))
	case identity.GrantStatusInvalidChain, identity.GrantStatusSubjectMismatch:
		return base + "\n  context grant no longer validates against the stored root (the cluster was likely re-rooted); rejoin with a fresh `pln join <token>`"
	}
	return base
}

func runContextShow(cmd *cobra.Command, args []string) error {
	name := resolveContextName()
	if len(args) == 1 {
		name = args[0]
	}
	entry, err := resolveContextBindings(name, defaultRootDir())
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	fmt.Fprintf(w, "context: %s\n", name)
	switch {
	case entry.Wire != "":
		fmt.Fprintf(w, "target:  %s\n", entry.Wire)
	case entry.Host != "":
		fmt.Fprintf(w, "target:  %s\n", entry.Host)
	}
	if entry.Dir != "" {
		fmt.Fprintf(w, "dir:     %s\n", entry.Dir)
	}

	grant, chk, ok := contextGrantCheck(entry.Dir)
	if !ok {
		fmt.Fprintln(w, "identity: none (run `pln join <token>`)")
		return nil
	}
	writeGrantIdentity(w, grant, chk)
	return nil
}

func writeGrantIdentity(w io.Writer, grant *identityv1.Grant, chk identity.GrantCheck) {
	claims := grant.GetClaims()
	subject := types.PeerKeyFromBytes(claims.GetSubjectPub())
	issuer := types.PeerKeyFromBytes(claims.GetIssuerPub())
	caps := claims.GetCapabilities()
	pub := caps.GetPublish()

	fmt.Fprintf(w, "identity: %s\n", subject.Short())
	fmt.Fprintf(w, "issuer:   %s\n", issuer.Short())
	fmt.Fprintf(w, "status:   %s\n", chk.Status)
	fmt.Fprintf(w, "not before: %s\n", chk.NotBefore.Format(certTimeFormat))
	if chk.GrantDeadline.IsZero() {
		fmt.Fprintln(w, "rejoin by:  never (admin grant)")
	} else {
		fmt.Fprintf(w, "rejoin by:  %s\n", chk.GrantDeadline.Format(certTimeFormat))
	}
	fmt.Fprintf(w, "caps:     delegate=%t admit=%t publish=%t max_depth=%d\n",
		caps.GetCanDelegate(), caps.GetCanAdmit(),
		pub.GetFunctions() || pub.GetBlobs() || pub.GetSites() || pub.GetServices(),
		caps.GetMaxDepth())
}
