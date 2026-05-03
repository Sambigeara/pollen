// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package plnfs

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"sync"
)

func SetGroupSocket(path string) error { return setPerm(path, 0o660) }

func SetGroupReadable(path string) error { return setPerm(path, 0o640) }

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

func setPerm(path string, mode os.FileMode) error {
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	if !system {
		return nil
	}
	return applyPlnOwnership(path)
}

// When root, both uid and gid are set; when non-root, only gid is changed.
func applyPlnOwnership(path string) error {
	plnOnce.Do(resolvePlnOwner)
	if !plnResolved {
		if os.Getuid() == 0 {
			return fmt.Errorf("chown %s: pln user not found; run `pln daemon install` first", path)
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
