//go:build linux

package config

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"sync"
	"syscall"
)

func setPerm(path string, mode os.FileMode) error {
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return setPlnGroup(path)
}

// SetGroupDir makes a directory traversable by the pln group (0770).
func SetGroupDir(path string) error { return setPerm(path, 0o770) }

// SetGroupReadable makes a file readable by the pln group (0640).
func SetGroupReadable(path string) error { return setPerm(path, 0o640) }

// SetGroupSocket makes a socket read-writable by the pln group (0660).
func SetGroupSocket(path string) error { return setPerm(path, 0o660) }

// EnsureDir creates a directory (and parents) with pln group ownership.
// Root sets full pln:pln ownership; non-root sets the group to pln.
func EnsureDir(path string) error {
	if err := os.MkdirAll(path, 0o770); err != nil { //nolint:mnd
		return fmt.Errorf("mkdir %s: %w", path, err)
	}
	if os.Getuid() == 0 {
		return SetGroupDir(path)
	}
	return setPlnGroup(path)
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

// setPlnGroup sets the group of path to the pln group. Root does full
// chown(pln:pln); non-root does chgrp only, tolerating EPERM when the
// caller isn't a member of the pln group.
func setPlnGroup(path string) error {
	plnOnce.Do(resolvePlnOwner)
	if !plnResolved {
		return nil
	}
	if os.Getuid() == 0 {
		return os.Chown(path, plnUID, plnGID)
	}
	if err := os.Chown(path, -1, plnGID); err != nil && !errors.Is(err, syscall.EPERM) {
		return err
	}
	return nil
}
