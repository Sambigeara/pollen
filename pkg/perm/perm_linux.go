//go:build linux

package perm

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"sync"
)

func setPerm(path string, mode os.FileMode) error {
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return chownIfRoot(path)
}

// SetGroupDir makes a directory traversable by the pln group (0770).
func SetGroupDir(path string) error { return setPerm(path, 0o770) }

// SetGroupReadable makes a file readable by the pln group (0640).
func SetGroupReadable(path string) error { return setPerm(path, 0o640) }

// SetGroupSocket makes a socket read-writable by the pln group (0660).
func SetGroupSocket(path string) error { return setPerm(path, 0o660) }

// SetPrivate makes a file accessible only by the pln user (0600).
func SetPrivate(path string) error { return setPerm(path, 0o600) }

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

func chownIfRoot(path string) error {
	if os.Getuid() != 0 {
		return nil
	}
	plnOnce.Do(resolvePlnOwner)
	if !plnResolved {
		return nil
	}
	return os.Chown(path, plnUID, plnGID)
}
