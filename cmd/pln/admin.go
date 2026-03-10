package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/node"
)

func newAdminCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Manage admin keys",
	}

	cmd.AddCommand(&cobra.Command{
		Use:    "keygen",
		Short:  "Generate the local admin key",
		Run:    runAdminKeygen,
		Hidden: true,
	})

	cmd.AddCommand(&cobra.Command{
		Use:    "set-cert <admin-cert-b64>",
		Short:  "Install a delegated admin certificate",
		Args:   cobra.ExactArgs(1),
		Run:    runAdminSetCert,
		Hidden: true,
	})

	return cmd
}

func runAdminKeygen(cmd *cobra.Command, _ []string) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	_, pub, err := auth.LoadAdminKey(pollenDir)
	switch {
	case err == nil:
		fmt.Fprintf(cmd.OutOrStdout(), "admin key already present\nadmin_pub: %s\n", hex.EncodeToString(pub))
	case errors.Is(err, os.ErrNotExist):
		_, pub, err = auth.LoadOrCreateAdminKey(pollenDir)
		if err != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
			os.Exit(1)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "generated local admin key\nadmin_pub: %s\n", hex.EncodeToString(pub))
	default:
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}
	fmt.Fprintln(cmd.OutOrStdout(), "note: this does not grant signing authority")
	fmt.Fprintln(cmd.OutOrStdout(), "install a delegated admin cert with `pln admin set-cert <admin-cert-b64>`")
}

func runAdminSetCert(cmd *cobra.Command, args []string) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	_, adminPub, err := auth.LoadAdminKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	_, nodePub, err := node.GenIdentityKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	creds, err := auth.LoadExistingNodeCredentials(pollenDir, nodePub, time.Now())
	if err != nil {
		if errors.Is(err, auth.ErrCredentialsNotFound) {
			fmt.Fprintln(cmd.ErrOrStderr(), "node credentials not initialized; run `pln init` or `pln join <token>` first")
		} else {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
		}
		os.Exit(1)
	}

	cert, err := auth.UnmarshalAdminCertBase64(strings.TrimSpace(args[0]))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	if err := auth.VerifyAdminCert(cert, creds.Trust, time.Now()); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}
	if !bytes.Equal(cert.GetClaims().GetAdminPub(), adminPub) {
		fmt.Fprintln(cmd.ErrOrStderr(), "admin cert subject mismatch")
		os.Exit(1)
	}

	if err := auth.SaveAdminCert(pollenDir, cert); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	fmt.Fprintln(cmd.OutOrStdout(), "delegated admin certificate installed")
}
