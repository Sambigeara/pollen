package perm

import (
	"fmt"
	"os"
)

// atomicWrite writes data to path atomically: tmp → chmod → fsync → rename.
// The mode is applied to the tmp file before rename, so the target path never
// exists with a permissive mode. On error the tmp file is cleaned up.
func atomicWrite(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"

	if err := os.WriteFile(tmp, data, mode); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}

	// Explicit chmod overrides umask, which may have masked bits set by
	// os.WriteFile above (e.g. group-read under a 0077 umask).
	if err := os.Chmod(tmp, mode); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("chmod %s: %w", tmp, err)
	}

	if err := chownIfRoot(tmp); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("chown %s: %w", tmp, err)
	}

	f, err := os.Open(tmp)
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("open %s for sync: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s → %s: %w", tmp, path, err)
	}

	return nil
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
