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

// EnsureDir creates a directory (and parents) with setgid and pln group
// ownership. The setgid bit is currently ineffective for files written via
// atomicWrite because renameio creates temp files in /tmp (same mount) rather
// than the target directory, so the renamed file never inherits the directory
// group. setPerm handles group ownership explicitly instead.
func EnsureDir(path string) error {
	fm := os.ModeSetgid | 0o770
	if err := os.MkdirAll(path, fm); err != nil {
		return fmt.Errorf("mkdir %s: %w", path, err)
	}
	// MkdirAll's mode is subject to umask, so the setgid bit and
	// permission bits may not survive. setPerm forces the exact mode
	// and, when root, chowns to pln:pln. Non-root users who don't
	// own the directory (e.g. CLI users in the pln group) can't chmod,
	// so skip if the mode already matches.
	if info, err := os.Stat(path); err == nil && info.Mode() == os.ModeDir|fm {
		return nil
	}
	return setPerm(path, fm)
}

// setPerm sets the mode and ownership so the pln daemon can access the file.
// When root, both uid and gid are set to pln:pln. When non-root, the gid is
// set to the pln group (keeping the caller's uid). This is necessary because
// renameio may create temp files in /tmp instead of the setgid target
// directory, so group inheritance cannot be relied upon.
func setPerm(path string, mode os.FileMode) error {
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	plnOnce.Do(resolvePlnOwner)
	if !plnResolved {
		if os.Getuid() == 0 {
			return fmt.Errorf("chown %s: pln user not found; run `pln provision` first", path)
		}
		return nil
	}
	uid := -1 // existing owner
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
