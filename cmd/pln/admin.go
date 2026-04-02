package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/plnfs"
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
	reexecAsRoot(cmd)
	pollenDir, _ := cmd.Flags().GetString("dir")
	if err := plnfs.EnsureDir(pollenDir); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	_, pub, err := auth.EnsureAdminKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "admin_pub: %s\n", hex.EncodeToString(pub))
	fmt.Fprintln(cmd.OutOrStdout(), "note: this does not grant signing authority")
	fmt.Fprintln(cmd.OutOrStdout(), "install a delegated admin cert with `pln admin set-cert <admin-cert-b64>`")
}

func runAdminSetCert(cmd *cobra.Command, args []string) {
	reexecAsRoot(cmd)
	pollenDir, _ := cmd.Flags().GetString("dir")
	if err := plnfs.EnsureDir(pollenDir); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	_, nodePub, err := auth.EnsureIdentityKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	creds, err := auth.LoadNodeCredentials(pollenDir)
	if err != nil {
		if errors.Is(err, auth.ErrCredentialsNotFound) {
			fmt.Fprintln(cmd.ErrOrStderr(), "node credentials not initialized; run `pln init` or `pln join <token>` first")
		} else {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
		}
		os.Exit(1)
	}

	if err := auth.InstallDelegationCert(pollenDir, args[0], creds.RootPub(), nodePub, time.Now()); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	fmt.Fprintln(cmd.OutOrStdout(), "delegated admin certificate installed")
}
