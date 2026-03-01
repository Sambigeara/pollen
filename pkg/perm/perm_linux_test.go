//go:build linux

package perm

import (
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
)

func TestPollenGIDGroupAbsent(t *testing.T) {
	// If the pollen group doesn't exist on this machine, pollenGID
	// must return (0, false, nil) — a clean no-op, not an error.
	_, err := user.LookupGroup(groupName)
	if err == nil {
		t.Skip("pln group exists on this host; skipping absent-group test")
	}

	gid, ok, lookupErr := pollenGID()
	if lookupErr != nil {
		t.Fatalf("pollenGID returned error for absent group: %v", lookupErr)
	}
	if ok {
		t.Fatalf("pollenGID returned ok=true for absent group (gid=%d)", gid)
	}
}

func TestSetGroupPermNoOpWhenGroupAbsent(t *testing.T) {
	_, err := user.LookupGroup(groupName)
	if err == nil {
		t.Skip("pln group exists on this host; skipping no-op test")
	}

	dir := t.TempDir()
	f := filepath.Join(dir, "testfile")
	if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	// All three functions should silently no-op.
	for _, fn := range []struct {
		name string
		call func(string) error
	}{
		{"SetGroupReadable", SetGroupReadable},
		{"SetGroupSocket", SetGroupSocket},
		{"SetGroupDir", SetGroupDir},
	} {
		if err := fn.call(f); err != nil {
			t.Errorf("%s returned error when group absent: %v", fn.name, err)
		}
	}

	// Mode should be unchanged (still 0600).
	info, err := os.Stat(f)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("mode changed to %04o; want 0600", got)
	}
}

func TestSetGroupPermAppliedWhenGroupPresent(t *testing.T) {
	grp, err := user.LookupGroup(groupName)
	if err != nil {
		t.Skip("pln group not found; skipping applied-permission test")
	}
	expectedGID, err := strconv.Atoi(grp.Gid)
	if err != nil {
		t.Fatalf("bad gid %q: %v", grp.Gid, err)
	}

	dir := t.TempDir()

	// Test each function's expected mode and group ownership.
	for _, tc := range []struct {
		name     string
		call     func(string) error
		wantMode os.FileMode
	}{
		{"SetGroupDir", SetGroupDir, 0o770},
		{"SetGroupReadable", SetGroupReadable, 0o640},
		{"SetGroupSocket", SetGroupSocket, 0o660},
	} {
		p := filepath.Join(dir, tc.name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
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

		stat := info.Sys().(*syscall.Stat_t) //nolint:forcetypeassert
		if int(stat.Gid) != expectedGID {
			t.Errorf("%s: gid = %d; want %d", tc.name, stat.Gid, expectedGID)
		}
	}
}
