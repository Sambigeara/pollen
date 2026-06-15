// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	shareSubdomainBlob     = "blob"
	shareSubdomainWorkload = "fn"
)

func newShareCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "share <name-or-hash>",
		Short: "Mint a shareable URL for a blob or workload",
		Long: `Issues a signed, time-limited access token authorising any caller
in possession of the URL to fetch a blob or invoke a workload. The URL
points at the configured HTTP gateway (default ` + "`pln.sh`" + `). Sharing
requires publisher authority on the target resource; the daemon refuses
to sign for resources the caller didn't publish.`,
		Example: "  pln share payload\n  pln share echo --ttl 24h",
		Args:    cobra.ExactArgs(1),
		RunE:    withEnv(runShare),
	}
	cmd.Flags().Duration("ttl", time.Hour, "Validity window")
	cmd.Flags().String("gateway", "", "Gateway base DNS (overrides the cluster's configured domain)")
	return cmd
}

func runShare(cmd *cobra.Command, args []string, env *cliEnv) error {
	arg := args[0]
	ttl, _ := cmd.Flags().GetDuration("ttl")
	gateway, _ := cmd.Flags().GetString("gateway")

	statusResp, err := env.client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		return err
	}
	st := statusResp.Msg
	if gateway == "" {
		gateway = st.GetGatewayDomain()
	}
	if gateway == "" {
		return errors.New("gateway domain not configured on the cluster; set `pln set static-http-domain` on the daemon or pass --gateway")
	}

	wl, wlErr := matchWorkloadArg(st.GetWorkloads(), arg)
	if wlErr != nil {
		return wlErr
	}
	blobHash, blobErr := matchBlobArg(st.GetBlobs(), arg)
	hasBlob := blobErr == nil
	var blobAmbiguous error
	if blobErr != nil && !strings.HasPrefix(blobErr.Error(), "no blob") {
		blobAmbiguous = blobErr
	}

	matches := 0
	var kinds []string
	if wl != nil {
		matches++
		kinds = append(kinds, "workload "+wl.GetName())
	}
	if hasBlob {
		matches++
		kinds = append(kinds, "blob "+blobHash[:shortHexLen])
	}
	if matches == 0 {
		if blobAmbiguous != nil {
			return blobAmbiguous
		}
		return notFoundErr("no workload or blob matching %q", arg)
	}
	if matches > 1 {
		return ambiguousErr("multiple matches for %q (%s); pass a more specific identifier", arg, strings.Join(kinds, ", "))
	}

	creds, err := identity.LoadCredentials(identity.IdentityPath(env.dir))
	if err != nil {
		return fmt.Errorf("load credentials: %w", err)
	}
	if creds == nil || creds.Grant() == nil {
		return errors.New("no credentials in this context; run `pln join` first")
	}
	localPub := creds.Grant().GetClaims().GetSubjectPub()

	// A share token rides the publisher's authority, so refuse to mint one
	// from an invalid grant and clamp the token horizon to the grant
	// deadline. The gateway re-checks the issuer's grant per request
	// (issuerGrantValid), so this can't extend access; it keeps the printed
	// URL's lifetime honest.
	if chk := identity.CheckGrant(creds.Grant(), creds.RootPub(), time.Now(), localPub, nil); !chk.Status.Valid() {
		return fmt.Errorf("cannot mint a share token: local grant is %s (%s); rejoin first", chk.Status, chk.Reason)
	}
	if dl := creds.Grant().GetClaims().GetGrantDeadlineUnix(); dl > 0 {
		maxTTL := time.Until(time.Unix(dl, 0))
		if maxTTL <= 0 {
			return errors.New("cannot mint a share token: local grant has expired; rejoin first")
		}
		if ttl > maxTTL {
			ttl = maxTTL
		}
	}

	resource, subdomain, err := buildShareResource(wl, hasBlob, blobHash, st.GetBlobs(), localPub)
	if err != nil {
		return err
	}

	priv, _, err := identity.EnsureIdentityKey(identity.IdentityPath(env.dir))
	if err != nil {
		return fmt.Errorf("load identity key: %w", err)
	}

	token, err := auth.SignAccessToken(priv, resource, time.Now(), ttl)
	if err != nil {
		return fmt.Errorf("sign access token: %w", err)
	}
	encoded, err := auth.EncodeAccessToken(token)
	if err != nil {
		return fmt.Errorf("encode access token: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "https://%s.%s/%s/%s\n", subdomain, gateway, types.ReservedBearerSlug, encoded)
	return nil
}

// buildShareResource resolves the share target to a ResourceID and the
// matching gateway subdomain. It also enforces the publisher-match
// invariant: only the publisher of a resource can mint a token for it
// (the gateway rejects mismatched issuer/publisher pairs at fetch time,
// so catching the mismatch here surfaces a useful error instead of a
// non-working URL).
func buildShareResource(wl *controlv1.WorkloadSummary, hasBlob bool, blobHash string, blobs []*controlv1.BlobSummary, localPub []byte) (*admissionv1.ResourceID, string, error) {
	switch {
	case wl != nil:
		if pub := wl.GetPublisher().GetPeerPub(); len(pub) > 0 && !bytes.Equal(pub, localPub) {
			return nil, "", fmt.Errorf("cannot share %q: published by another peer; only the publisher's context can mint share tokens", wl.GetName())
		}
		hashBytes, err := hex.DecodeString(wl.GetHash())
		if err != nil {
			return nil, "", fmt.Errorf("decode workload hash: %w", err)
		}
		return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
			Name: wl.GetName(),
			Hash: hashBytes,
		}}}, shareSubdomainWorkload, nil
	case hasBlob:
		name := ""
		var pubKey []byte
		for _, b := range blobs {
			if b.GetHash() == blobHash {
				name = b.GetName()
				pubKey = b.GetPublisher().GetPeerPub()
				break
			}
		}
		if len(pubKey) > 0 && !bytes.Equal(pubKey, localPub) {
			return nil, "", fmt.Errorf("cannot share blob %s: published by another peer; only the publisher's context can mint share tokens", blobHash[:shortHexLen])
		}
		digestBytes, err := hex.DecodeString(blobHash)
		if err != nil {
			return nil, "", fmt.Errorf("decode blob hash: %w", err)
		}
		return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{
			Name:   name,
			Digest: digestBytes,
		}}}, shareSubdomainBlob, nil
	}
	return nil, "", fmt.Errorf("no share target")
}
