//go:build linux

package perm

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestSetPermAppliesMode(t *testing.T) {
	dir := t.TempDir()

	for _, tc := range []struct {
		name     string
		call     func(string) error
		wantMode os.FileMode
	}{
		{"SetGroupDir", SetGroupDir, 0o770},
		{"SetGroupReadable", SetGroupReadable, 0o640},
		{"SetGroupSocket", SetGroupSocket, 0o660},
		{"SetPrivate", SetPrivate, 0o600},
	} {
		p := filepath.Join(dir, tc.name)
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}

		if err := tc.call(p); err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}

		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}

		if got := info.Mode().Perm(); got != tc.wantMode {
			t.Errorf("%s: mode = %04o; want %04o", tc.name, got, tc.wantMode)
		}
	}
}

// TestWriteOverridesUmask verifies that WritePrivate and WriteGroupReadable
// produce exact modes even under a restrictive umask (e.g. 0077).
func TestWriteOverridesUmask(t *testing.T) {
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)

	dir := t.TempDir()

	for _, tc := range []struct {
		name     string
		write    func(string, []byte) error
		wantMode os.FileMode
	}{
		{"WritePrivate", WritePrivate, 0o600},
		{"WriteGroupReadable", WriteGroupReadable, 0o640},
	} {
		p := filepath.Join(dir, tc.name)
		if err := tc.write(p, []byte("data")); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}

		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}

		if got := info.Mode().Perm(); got != tc.wantMode {
			t.Errorf("%s under umask 0077: mode = %04o; want %04o", tc.name, got, tc.wantMode)
		}
	}
}
