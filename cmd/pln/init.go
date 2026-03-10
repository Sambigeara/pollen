package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/node"
)

func newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize local root cluster state",
		Run:   runInit,
	}
}

func newPurgeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "purge",
		Short: "Delete local cluster state",
		Run:   runPurge,
	}
	cmd.Flags().Bool("all", false, "Also delete local node identity keys")
	cmd.Flags().Bool("yes", false, "Skip interactive confirmation")
	return cmd
}

func runInit(cmd *cobra.Command, _ []string) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	running, err := nodeSocketActive(filepath.Join(pollenDir, socketName))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}
	if running {
		fmt.Fprintln(cmd.ErrOrStderr(), "local node is running; run `pln down` before initializing")
		os.Exit(1)
	}

	_, pub, err := node.GenIdentityKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	existing, err := auth.LoadExistingNodeCredentials(pollenDir, pub, time.Now())
	if err == nil {
		_, adminPub, adminErr := auth.LoadAdminKey(pollenDir)
		if adminErr != nil && !errors.Is(adminErr, os.ErrNotExist) {
			fmt.Fprintln(cmd.ErrOrStderr(), adminErr)
			os.Exit(1)
		}

		if adminErr == nil && slices.Equal(adminPub, existing.Trust.GetRootPub()) {
			fmt.Fprintf(cmd.OutOrStdout(), "already initialized as root cluster\nroot_pub: %s\ncluster_id: %s\n",
				hex.EncodeToString(adminPub),
				hex.EncodeToString(existing.Trust.GetClusterId()),
			)
			return
		}

		fmt.Fprintln(cmd.ErrOrStderr(), "node is already enrolled in a cluster; run `pln purge` before initializing a new root cluster")
		os.Exit(1)
	}
	if !errors.Is(err, auth.ErrCredentialsNotFound) {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		fmt.Fprintln(cmd.ErrOrStderr(), "run `pln purge` to reset local state")
		os.Exit(1)
	}

	cfg := loadConfigOrDefault(pollenDir)
	certTTLs := cfg.CertTTLs

	creds, err := auth.EnsureLocalRootCredentials(pollenDir, pub, time.Now(), certTTLs.MembershipTTL(), certTTLs.AdminTTL())
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	_, adminPub, err := auth.LoadAdminKey(pollenDir)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "initialized root cluster\nroot_pub: %s\ncluster_id: %s\n",
		hex.EncodeToString(adminPub),
		hex.EncodeToString(creds.Trust.GetClusterId()),
	)

	ensureSudoUserInPlnGroup(cmd)
}

func runPurge(cmd *cobra.Command, _ []string) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	all, _ := cmd.Flags().GetBool("all")
	confirmed, _ := cmd.Flags().GetBool("yes")

	running, err := nodeSocketActive(filepath.Join(pollenDir, socketName))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}
	if running {
		fmt.Fprintln(cmd.ErrOrStderr(), "local node is running; run `pln down` before purging state")
		os.Exit(1)
	}

	if !confirmed && !confirmPurge(cmd, all) {
		return
	}

	paths := append(clusterStatePaths(pollenDir), filepath.Join(pollenDir, socketName))
	if all {
		paths = append(paths,
			filepath.Join(pollenDir, "keys", "ed25519.key"),
			filepath.Join(pollenDir, "keys", "ed25519.pub"),
		)
	}

	for _, path := range paths {
		if removeErr := os.RemoveAll(path); removeErr != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "remove %s: %v\n", path, removeErr)
			os.Exit(1)
		}
	}

	keysDir := filepath.Join(pollenDir, "keys")
	if entries, err := os.ReadDir(keysDir); err == nil && len(entries) == 0 {
		if err := os.Remove(keysDir); err != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
			os.Exit(1)
		}
	}

	fmt.Fprintln(cmd.OutOrStdout(), "local state purged")
	if all {
		fmt.Fprintln(cmd.OutOrStdout(), "identity keys removed")
	}
}

func confirmPurge(cmd *cobra.Command, all bool) bool {
	fmt.Fprintln(cmd.ErrOrStderr(), "This will delete local cluster, admin, and runtime state.")
	if all {
		fmt.Fprintln(cmd.ErrOrStderr(), "It will also delete local node identity keys.")
	} else {
		fmt.Fprintln(cmd.ErrOrStderr(), "Node identity keys will be kept.")
	}
	fmt.Fprint(cmd.ErrOrStderr(), "Type \"yes\" to continue: ")

	scanner := bufio.NewScanner(cmd.InOrStdin())
	if !scanner.Scan() {
		if scanErr := scanner.Err(); scanErr != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), scanErr)
		} else {
			fmt.Fprintln(cmd.ErrOrStderr(), "aborted")
		}
		return false
	}

	if strings.TrimSpace(strings.ToLower(scanner.Text())) != "yes" {
		fmt.Fprintln(cmd.ErrOrStderr(), "aborted")
		return false
	}
	return true
}
