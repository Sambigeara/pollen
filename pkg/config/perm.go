package config

import (
	"fmt"
	"os"

	"github.com/google/renameio/v2"
)

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := renameio.WriteFile(path, data, mode); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return setPlnGroup(path)
}

// WritePrivate atomically writes data to path with mode 0600.
func WritePrivate(path string, data []byte) error {
	return atomicWrite(path, data, 0o600) //nolint:mnd
}

// WriteGroupReadable atomically writes data to path with mode 0640.
func WriteGroupReadable(path string, data []byte) error {
	return atomicWrite(path, data, 0o640) //nolint:mnd
}

// WriteGroupWritable atomically writes data to path with mode 0660.
func WriteGroupWritable(path string, data []byte) error {
	return atomicWrite(path, data, 0o660) //nolint:mnd
}
