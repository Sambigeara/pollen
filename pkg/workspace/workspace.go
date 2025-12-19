package workspace

import (
	"fmt"
	"os"
)

const (
	keyDirPerm = 0o700
)

func EnsurePollenDir(dir string) (string, error) {
	if err := os.MkdirAll(dir, keyDirPerm); err != nil {
		return "", fmt.Errorf("unable to create pollen dir: %w", err)
	}

	return dir, nil
}
