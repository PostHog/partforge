package freeze

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScanFindsParts(t *testing.T) {
	root := t.TempDir()
	part := filepath.Join(root, "freeze-1", "store", "abc", "all_1_1_0")
	if err := os.MkdirAll(part, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"checksums.txt", "columns.txt"} {
		if err := os.WriteFile(filepath.Join(part, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	parts, err := Scan("default", filepath.Join(root, "freeze-1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 {
		t.Fatalf("got %d parts", len(parts))
	}
	if parts[0].Disk != "default" {
		t.Fatalf("unexpected disk %q", parts[0].Disk)
	}
	if parts[0].RelativePath != "store/abc/all_1_1_0" {
		t.Fatalf("unexpected relative path %q", parts[0].RelativePath)
	}
}

func TestScanDisksFindsPartsOnMultipleDisks(t *testing.T) {
	root := t.TempDir()
	for _, disk := range []string{"disk-a", "disk-b"} {
		part := filepath.Join(root, disk, "shadow", "freeze-1", "store", disk, "all_1_1_0")
		if err := os.MkdirAll(part, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"checksums.txt", "columns.txt"} {
			if err := os.WriteFile(filepath.Join(part, name), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	parts, err := ScanDisks([]Disk{
		{Name: "disk-a", Path: filepath.Join(root, "disk-a")},
		{Name: "disk-b", Path: filepath.Join(root, "disk-b")},
	}, "freeze-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 {
		t.Fatalf("got %d parts", len(parts))
	}
	if parts[0].Disk != "disk-a" || parts[1].Disk != "disk-b" {
		t.Fatalf("unexpected disks: %+v", parts)
	}
}

func TestValidateLocalDiskRejectsS3(t *testing.T) {
	err := validateLocalDisk(Disk{Name: "remote", Path: "/var/lib/clickhouse/disks/remote", Type: "s3"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "unsupported S3") {
		t.Fatalf("unexpected error: %v", err)
	}
}
