//go:build !linux

package server

func setSocketGroupPermissions(_ string) error { return nil }
