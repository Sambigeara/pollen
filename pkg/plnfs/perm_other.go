//go:build !linux

package plnfs

import "os"

func SetGroupSocket(_ string) error { return nil }

func setPerm(path string, mode os.FileMode) error {
	return os.Chmod(path, mode)
}

// EnsureDir creates a directory (and parents) with mode 0700.
// On non-Linux platforms no group ownership is set.
func EnsureDir(path string) error {
	return os.MkdirAll(path, 0o700) //nolint:mnd
}
