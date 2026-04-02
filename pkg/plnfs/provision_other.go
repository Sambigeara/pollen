//go:build !linux

package plnfs

func Provision(string) error { return nil }

func AddUserToPlnGroup(string) error { return nil }
