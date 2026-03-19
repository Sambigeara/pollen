package config

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

func TestEnsureDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b")

	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatal("expected directory")
	}

	if runtime.GOOS == "linux" {
		if mode := info.Mode().Perm(); mode != 0o770 {
			t.Errorf("mode = %04o; want 0770", mode)
		}
	}

	// Idempotent: calling again on existing dir should not fail.
	if err := EnsureDir(dir); err != nil {
		t.Fatalf("idempotent call failed: %v", err)
	}
}

func TestWriteGroupWritable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shared")

	data := []byte("shared data")
	if err := WriteGroupWritable(path, data); err != nil {
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
		if mode := info.Mode().Perm(); mode != 0o660 {
			t.Errorf("mode = %04o; want 0660", mode)
		}
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
