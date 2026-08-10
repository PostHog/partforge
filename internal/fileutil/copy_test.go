package fileutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMoveDirMovesDirectory(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "nested", "dst")
	if err := os.MkdirAll(filepath.Join(src, "child"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "child", "file.txt"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := MoveDir(src, dst); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("source still exists or unexpected stat error: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "child", "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "content" {
		t.Fatalf("moved content = %q", got)
	}
}

func TestMoveDirRejectsExistingDestination(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "dst")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}

	err := MoveDir(src, dst)
	if err == nil {
		t.Fatal("expected destination exists error")
	}
	if !strings.Contains(err.Error(), "destination already exists") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConcatFiles(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	if err := os.WriteFile(first, []byte("base"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("-suffix"), 0o644); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "out", "combined")
	if err := ConcatFiles(destination, []string{first, second}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "base-suffix" {
		t.Fatalf("combined contents = %q", got)
	}
}
