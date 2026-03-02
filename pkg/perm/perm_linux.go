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

const userName = "pln"

func pollenIDs() (uid, gid int, ok bool, err error) {
	u, err := user.Lookup(userName)
	if err != nil {
		if errors.As(err, new(user.UnknownUserError)) {
			return 0, 0, false, nil
		}
		return 0, 0, false, fmt.Errorf("lookup user %s: %w", userName, err)
	}
	uid, err = strconv.Atoi(u.Uid)
	if err != nil {
		return 0, 0, false, fmt.Errorf("parse uid %s: %w", u.Uid, err)
	}
	gid, err = strconv.Atoi(u.Gid)
	if err != nil {
		return 0, 0, false, fmt.Errorf("parse gid %s: %w", u.Gid, err)
	}
	return uid, gid, true, nil
}

func setPerm(path string, mode os.FileMode) error {
	uid, gid, ok, err := pollenIDs()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := os.Chown(path, uid, gid); err != nil {
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
func SetGroupDir(path string) error { return setPerm(path, 0o770) }

// SetGroupReadable makes a file readable by the pln group (0640).
func SetGroupReadable(path string) error { return setPerm(path, 0o640) }

// SetGroupSocket makes a socket read-writable by the pln group (0660).
func SetGroupSocket(path string) error { return setPerm(path, 0o660) }

// SetPrivate makes a file accessible only by the pln user (0600).
func SetPrivate(path string) error { return setPerm(path, 0o600) }
