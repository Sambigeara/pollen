package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	keyDirPerm = 0o700
)

func EnsurePollenDir(dir string) (string, error) {
	if err := os.MkdirAll(dir, keyDirPerm); err != nil {
		if errors.Is(err, os.ErrPermission) && strings.HasPrefix(dir, "/var/lib/pln") {
			return "", fmt.Errorf("unable to create pollen dir: %w\n"+
				"  %s requires membership in the \"pln\" group\n"+
				"  fix: sudo usermod -aG pln $(whoami) && newgrp pln\n"+
				"  alternatively, set PLN_DIR to use a different directory", err, dir)
		}
		return "", fmt.Errorf("unable to create pollen dir: %w", err)
	}

	return dir, nil
}
