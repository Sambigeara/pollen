//go:build linux

package server

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
)

func setSocketGroupPermissions(path string) error {
	grp, err := user.LookupGroup("pollen")
	if err != nil {
		var unknownGroup user.UnknownGroupError
		if errors.As(err, &unknownGroup) {
			return nil
		}
		return fmt.Errorf("lookup pollen group: %w", err)
	}
	gid, err := strconv.Atoi(grp.Gid)
	if err != nil {
		return fmt.Errorf("parse pollen group gid %q: %w", grp.Gid, err)
	}
	if err := os.Chown(path, -1, gid); err != nil {
		return fmt.Errorf("chown socket: %w", err)
	}
	if err := os.Chmod(path, 0o660); err != nil {
		return fmt.Errorf("chmod socket: %w", err)
	}
	return nil
}
