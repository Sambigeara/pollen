package plnfs

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

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
