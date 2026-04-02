package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/plnfs"
	"github.com/sambigeara/pollen/pkg/transport"
)

func newJoinCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "join <token>",
		Short: "Join a cluster using a join or invite token",
		Args:  cobra.ExactArgs(1),
		Run:   runJoin,
	}
	cmd.Flags().Bool("no-up", false, "Enroll credentials without starting the daemon")
	cmd.Flags().Bool("public", false, "Mark this node as publicly accessible (relay)")
	return cmd
}

// enrollToken resolves a join/invite token, enrolls node credentials (purging
// stale cluster state on cluster switch), saves bootstrap peers from the token,
// and fixes file ownership.
func enrollToken(ctx context.Context, pollenDir, rawToken string) error {
	privKey, pubKey, err := auth.EnsureIdentityKey(pollenDir)
	if err != nil {
		return err
	}

	tkn, err := resolveJoinToken(ctx, privKey, rawToken)
	if err != nil {
		return fmt.Errorf("resolve token: %w", err)
	}

	_, credErr := auth.EnrollNodeCredentials(pollenDir, pubKey, tkn, time.Now())
	if credErr != nil {
		if errors.Is(credErr, auth.ErrDifferentCluster) {
			for _, p := range clusterStatePaths(pollenDir) {
				if purgeErr := os.RemoveAll(p); purgeErr != nil {
					return fmt.Errorf("purge old cluster state: remove %s: %w", p, purgeErr)
				}
			}
			_, credErr = auth.EnrollNodeCredentials(pollenDir, pubKey, tkn, time.Now())
		}
		if credErr != nil {
			return fmt.Errorf("enroll credentials: %w", credErr)
		}
	}

	for _, bp := range tkn.GetClaims().GetBootstrap() {
		if saveErr := saveBootstrapPeer(pollenDir, bp); saveErr != nil {
			return fmt.Errorf("save bootstrap peer: %w", saveErr)
		}
	}

	return nil
}

func runJoin(cmd *cobra.Command, args []string) {
	reexecAsRoot(cmd)
	pollenDir, _ := cmd.Flags().GetString("dir")
	if err := plnfs.EnsureDir(pollenDir); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	noUp, _ := cmd.Flags().GetBool("no-up")
	public, _ := cmd.Flags().GetBool("public")

	if err := enrollJoin(cmd, pollenDir, args[0], public); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	if noUp {
		fmt.Fprintln(cmd.OutOrStdout(), "credentials enrolled; run `pln up -d` to start the node")
		return
	}

	sockPath := filepath.Join(pollenDir, socketName)
	if active, _ := nodeSocketActive(sockPath); active {
		if err := servicectl("restart", cmd); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "joined cluster but failed to restart daemon: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "joined cluster; daemon restarted")
		return
	}

	fmt.Fprintln(cmd.OutOrStdout(), "credentials enrolled; starting daemon")
	if err := servicectl("start", cmd); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}
}

func enrollJoin(cmd *cobra.Command, pollenDir, rawToken string, public bool) error {
	if err := enrollToken(cmd.Context(), pollenDir, rawToken); err != nil {
		return err
	}
	if public {
		if err := setConfigPublic(pollenDir, true); err != nil {
			return fmt.Errorf("set public flag: %w", err)
		}
	}
	return nil
}

func resolveJoinToken(ctx context.Context, priv ed25519.PrivateKey, encoded string) (*admissionv1.JoinToken, error) {
	trimmed := strings.TrimSpace(encoded)

	joinToken, joinErr := auth.DecodeJoinToken(trimmed)
	if joinErr == nil {
		if _, err := auth.VerifyJoinToken(joinToken, priv.Public().(ed25519.PublicKey), time.Now()); err == nil { //nolint:forcetypeassert
			return joinToken, nil
		}
	}

	inviteToken, inviteErr := auth.DecodeInviteToken(trimmed)
	if inviteErr != nil {
		if joinErr != nil {
			return nil, fmt.Errorf("failed to decode join token (%w) or invite token (%w)", joinErr, inviteErr)
		}
		return nil, fmt.Errorf("failed to decode invite token (%w)", inviteErr)
	}

	return transport.RedeemInvite(ctx, priv, inviteToken)
}
