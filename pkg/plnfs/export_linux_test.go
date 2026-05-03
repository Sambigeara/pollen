// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package plnfs

func EnableSystemMode() func() {
	old := system
	system = true
	return func() { system = old }
}
