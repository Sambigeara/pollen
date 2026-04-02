package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/sambigeara/pollen/pkg/plnfs"
)

func newProvisionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "provision",
		Short: "Create the pln system user, group, and state directories (Linux, requires root)",
		Run:   runProvision,
	}
}

func runProvision(cmd *cobra.Command, _ []string) {
	if runtime.GOOS != osLinux {
		fmt.Fprintln(cmd.OutOrStdout(), "provisioning is not required on this platform")
		return
	}

	if os.Getuid() != 0 {
		fmt.Fprintln(cmd.ErrOrStderr(), "pln provision must be run as root")
		os.Exit(1)
	}

	// Always default to the system directory. The root command's --dir
	// default resolves SUDO_USER's home which is wrong for provisioning
	// a fresh machine where /var/lib/pln doesn't exist yet.
	dir := "/var/lib/pln"
	if cmd.Flags().Changed("dir") {
		dir, _ = cmd.Flags().GetString("dir")
	}

	if err := plnfs.Provision(dir); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	if sudoUser := os.Getenv("SUDO_USER"); sudoUser != "" {
		if err := plnfs.AddUserToPlnGroup(sudoUser); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: %v\n", err)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "added %s to the pln group -- log out and back in for CLI access without sudo\n", sudoUser)
		}
	}

	fmt.Fprintln(cmd.OutOrStdout(), "provisioned "+dir)
}
