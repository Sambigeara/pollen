//go:build linux

package perm

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"syscall"
)

const groupName = "pln"

func pollenGID() (int, bool, error) {
	grp, err := user.LookupGroup(groupName)
	if err != nil {
		if errors.As(err, new(user.UnknownGroupError)) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("lookup group %s: %w", groupName, err)
	}
	gid, err := strconv.Atoi(grp.Gid)
	if err != nil {
		return 0, false, fmt.Errorf("parse gid %s: %w", grp.Gid, err)
	}
	return gid, true, nil
}

func setGroupPerm(path string, mode os.FileMode) error {
	gid, ok, err := pollenGID()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := os.Chown(path, -1, gid); err != nil {
		if !errors.Is(err, syscall.EPERM) {
			return fmt.Errorf("chown %s: %w", path, err)
		}
	}
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return nil
}

// SetGroupDir makes a directory traversable by the pln group (0770).
func SetGroupDir(path string) error { return setGroupPerm(path, 0o770) }

// SetGroupReadable makes a file readable by the pln group (0640).
func SetGroupReadable(path string) error { return setGroupPerm(path, 0o640) }

// SetGroupSocket makes a socket read-writable by the pln group (0660).
func SetGroupSocket(path string) error { return setGroupPerm(path, 0o660) }
