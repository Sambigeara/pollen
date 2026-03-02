//go:build linux

package perm

import (
	"fmt"
	"os"
)

func setPerm(path string, mode os.FileMode) error {
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
