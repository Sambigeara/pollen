package workspace

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	pollenRootDir = ".pollen"
	socketName    = "pollen.sock"
)

func EnsurePollenDir() (string, error) {
	base, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("unable to retrieve user config dir: %v", err)
	}

	pollenDir := filepath.Join(base, pollenRootDir)

	if err := os.MkdirAll(pollenDir, 0o700); err != nil {
		return "", fmt.Errorf("unable to create pollen dir: %v", err)
	}

	return pollenDir, nil
}
