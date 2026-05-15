// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
	"github.com/sambigeara/pollen/pkg/auth"
)

// renewHalfLifeDivisor triggers a proactive renewal once a wire-mode
// cert has less than 1/N of its lifetime remaining (here, past the
// half-life). Renewing early keeps the cert comfortably valid across
// network blips without waiting for it to lapse into NeedsRenewal.
const renewHalfLifeDivisor = 2

// ensureWireCertFresh inspects the context cert before a wire-mode RPC
// and renews it in place when it is past not_after or past its
// half-life. It is a no-op for healthy certs and never runs for
// non-wire targets (the daemon owns its own cert lifecycle). A failed
// proactive renewal is swallowed: the still-valid cert authenticates
// fine. A failed required renewal, or a hard-expired cert, returns a
// user-facing error pointing at the rejoin path.
func ensureWireCertFresh(ctx context.Context, dir, host, baseURL string) error {
	identityDir := auth.IdentityPath(dir)
	creds, err := auth.LoadNodeCredentials(identityDir)
	// No creds in this context yet; the dial will surface a clearer
	// "run pln join first" error than we could here, so swallow.
	if err != nil || creds == nil || creds.Cert() == nil {
		return nil //nolint:nilerr
	}

	chk := auth.CheckCert(creds.Cert(), creds.RootPub(), time.Now(), nil, nil)
	switch chk.Status {
	case auth.CertStatusExpired:
		return fmt.Errorf("context cert expired beyond renewal (%s); rejoin with `pln join <token>` (mint one on the cluster with `pln invite`)", chk.Reason)
	case auth.CertStatusNeedsRenewal:
		// Required: every non-RenewCert RPC is refused until renewed.
	case auth.CertStatusOK:
		lifetime := chk.NotAfter.Sub(chk.NotBefore)
		if time.Until(chk.NotAfter) > lifetime/renewHalfLifeDivisor {
			return nil
		}
		// Past half-life: renew proactively below.
	default:
		// NotYetValid / Revoked / InvalidChain / SubjectMismatch: not a
		// renewal situation. Let the handshake surface the precise cause.
		return nil
	}

	required := chk.Status == auth.CertStatusNeedsRenewal
	client := controlv1connect.NewControlServiceClient(
		&http.Client{Transport: &http2.Transport{AllowHTTP: true, DialTLS: dialTLSFunc(dir, host)}},
		baseURL,
		connect.WithGRPC(),
	)
	resp, err := client.RenewCert(ctx, connect.NewRequest(&controlv1.RenewCertRequest{}))
	if err != nil {
		if required {
			return fmt.Errorf("cert renewal failed: %w", err)
		}
		return nil
	}
	newCert := resp.Msg.GetCert()
	if newCert == nil {
		if required {
			return errors.New("cert renewal returned no certificate")
		}
		return nil
	}
	if vc := auth.CheckCert(newCert, creds.RootPub(), time.Now(), nil, nil); !vc.Status.CanAuthenticate() {
		return fmt.Errorf("renewed cert invalid: %s", vc.Reason)
	}
	if err := auth.SaveNodeCredentials(identityDir, auth.NewNodeCredentials(creds.RootPub(), newCert)); err != nil {
		return fmt.Errorf("persist renewed cert: %w", err)
	}
	return nil
}
