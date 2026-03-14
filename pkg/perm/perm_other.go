//go:build !linux

package perm

import "os"

func setGroupDir(_ string) error      { return nil }
func SetGroupReadable(_ string) error { return nil }
func SetGroupSocket(_ string) error   { return nil }
func SetPrivate(_ string) error       { return nil }
func setPlnGroup(_ string) error      { return nil }

// EnsureDir creates a directory (and parents) with mode 0700.
// On non-Linux platforms no group ownership is set.
func EnsureDir(path string) error {
	return os.MkdirAll(path, 0o700) //nolint:mnd
}
