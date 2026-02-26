//go:build !linux

package perm

func SetGroupDir(_ string) error      { return nil }
func SetGroupReadable(_ string) error { return nil }
func SetGroupSocket(_ string) error   { return nil }
