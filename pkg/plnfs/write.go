package plnfs

import (
	"fmt"
	"os"

	"github.com/google/renameio/v2"
)

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := renameio.WriteFile(path, data, mode); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	// renameio creates the temp file in the same directory, so setgid
	// group inheritance works. setPerm is still needed to override umask
	// on the mode bits and, when root, to chown to pln:pln.
	return setPerm(path, mode)
}

// WriteGroupReadable atomically writes data to path with mode 0640.
func WriteGroupReadable(path string, data []byte) error {
	return atomicWrite(path, data, 0o640) //nolint:mnd
}

// WriteGroupWritable atomically writes data to path with mode 0660.
func WriteGroupWritable(path string, data []byte) error {
	return atomicWrite(path, data, 0o660) //nolint:mnd
}
