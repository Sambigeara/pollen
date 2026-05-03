// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package plnfs

import "os"

func SetGroupSocket(_ string) error { return nil }

func SetGroupReadable(path string) error { return setPerm(path, 0o640) } //nolint:mnd

func setPerm(path string, mode os.FileMode) error {
	return os.Chmod(path, mode)
}

func EnsureDir(path string) error {
	return os.MkdirAll(path, 0o700) //nolint:mnd
}
