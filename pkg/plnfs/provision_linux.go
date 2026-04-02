//go:build linux

package plnfs

import (
	"fmt"
	"os/exec"
	"os/user"
	"path/filepath"
)

// Provision creates the pln system group and user, then ensures the
// standard state directories exist with correct ownership and modes.
// Idempotent.
func Provision(dir string) error {
	if err := ensureGroup("pln"); err != nil {
		return err
	}
	if err := ensureUser("pln", dir); err != nil {
		return err
	}
	for _, sub := range []string{"", "keys", "cas"} {
		if err := EnsureDir(filepath.Join(dir, sub)); err != nil {
			return err
		}
	}
	return nil
}

// AddUserToPlnGroup adds the named user to the pln group.
func AddUserToPlnGroup(username string) error {
	if out, err := exec.Command("usermod", "-aG", "pln", username).CombinedOutput(); err != nil {
		return fmt.Errorf("add %s to pln group: %w\n%s", username, err, out)
	}
	return nil
}

func ensureGroup(name string) error {
	if _, err := user.LookupGroup(name); err == nil {
		return nil
	}
	if out, err := exec.Command("groupadd", "--system", name).CombinedOutput(); err != nil {
		return fmt.Errorf("create system group %s: %w\n%s", name, err, out)
	}
	return nil
}

func ensureUser(name, home string) error {
	if _, err := user.Lookup(name); err == nil {
		return nil
	}
	if out, err := exec.Command(
		"useradd", "-r",
		"-d", home,
		"-s", "/usr/sbin/nologin",
		"-g", name,
		name,
	).CombinedOutput(); err != nil {
		return fmt.Errorf("create system user %s: %w\n%s", name, err, out)
	}
	return nil
}
