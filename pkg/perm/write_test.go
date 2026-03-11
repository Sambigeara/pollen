package perm

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestWritePrivate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")

	data := []byte("sensitive data")
	if err := WritePrivate(path, data); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("content = %q; want %q", got, data)
	}

	if runtime.GOOS == "linux" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if mode := info.Mode().Perm(); mode != 0o600 {
			t.Errorf("mode = %04o; want 0600", mode)
		}
	}
}

func TestWritePrivateAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")

	original := []byte("original")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}

	// Overwrite with new content — old file should be replaced.
	updated := []byte("updated")
	if err := WritePrivate(path, updated); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(updated) {
		t.Errorf("content = %q; want %q", got, updated)
	}

	// Tmp file should not linger.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("tmp file should not exist after successful write")
	}
}

func TestWriteGroupReadable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state")

	data := []byte("cluster state")
	if err := WriteGroupReadable(path, data); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("content = %q; want %q", got, data)
	}

	if runtime.GOOS == "linux" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if mode := info.Mode().Perm(); mode != 0o640 {
			t.Errorf("mode = %04o; want 0640", mode)
		}
	}
}
