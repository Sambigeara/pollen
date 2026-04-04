//go:build linux

package plnfs

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"sync"
)

// SetGroupSocket makes a socket read-writable by the pln group (0660).
func SetGroupSocket(path string) error { return setPerm(path, 0o660) }

// EnsureDir creates a directory (and parents). In system mode it applies
// setgid + pln group ownership so the daemon and CLI users in the pln
// group can share access. In user mode it uses 0700 and skips group
// ownership.
func EnsureDir(path string) error {
	if !system {
		if err := os.MkdirAll(path, 0o700); err != nil { //nolint:mnd
			return fmt.Errorf("mkdir %s: %w", path, err)
		}
		return os.Chmod(path, 0o700) //nolint:mnd
	}
	fm := os.ModeSetgid | 0o770
	if err := os.MkdirAll(path, fm); err != nil {
		return fmt.Errorf("mkdir %s: %w", path, err)
	}
	// MkdirAll's mode is subject to umask, so the setgid bit and
	// permission bits may not survive. setPerm forces the exact mode
	// and, when root, chowns to pln:pln.
	if info, err := os.Stat(path); err == nil && info.Mode() == os.ModeDir|fm {
		return nil
	}
	return setPerm(path, fm)
}

// setPerm sets the mode and, in system mode, the ownership so the pln
// daemon can access the file.
func setPerm(path string, mode os.FileMode) error {
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	if !system {
		return nil
	}
	return applyPlnOwnership(path)
}

// applyPlnOwnership chowns a path to pln:pln. When root, both uid and
// gid are set; when non-root, only the gid is changed (keeping the
// caller's uid). Exported via export_linux_test.go for Provision.
func applyPlnOwnership(path string) error {
	plnOnce.Do(resolvePlnOwner)
	if !plnResolved {
		if os.Getuid() == 0 {
			return fmt.Errorf("chown %s: pln user not found; run `pln provision` first", path)
		}
		return nil
	}
	uid := -1
	if os.Getuid() == 0 {
		uid = plnUID
	}
	return os.Chown(path, uid, plnGID)
}

var (
	plnOnce     sync.Once
	plnUID      int
	plnGID      int
	plnResolved bool
)

func resolvePlnOwner() {
	u, err := user.Lookup("pln")
	if err != nil {
		return
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return
	}
	plnUID = uid
	plnGID = gid
	plnResolved = true
}
