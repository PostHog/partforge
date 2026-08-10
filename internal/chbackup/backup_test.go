package chbackup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareResolvesFullBackupDeduplication(t *testing.T) {
	index := writeIndex(t, `<config><version>1</version><uuid>full</uuid><contents>`+
		`<file><name>data/src/events/all_1_1_0/checksums.txt</name><size>10</size><checksum>a</checksum></file>`+
		`<file><name>data/src/events/all_1_1_0/empty.bin</name><size>0</size></file>`+
		`<file><name>data/src/events/all_2_2_0/checksums.txt</name><size>10</size><checksum>a</checksum><data_file>data/src/events/all_1_1_0/checksums.txt</data_file></file>`+
		`<file><name>metadata/src/events.sql</name><size>50</size><checksum>b</checksum></file>`+
		`</contents></config>`)
	plan, err := Prepare([]Layer{{Bucket: "backups", Prefix: "full", IndexPath: index}}, "src", "events")
	if err != nil {
		t.Fatal(err)
	}
	var parts []Part
	if err := plan.ScanParts(func(part Part) error {
		parts = append(parts, part)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if plan.Info().PartCount != 2 || len(parts) != 2 {
		t.Fatalf("parts = %d/%d, want 2", plan.Info().PartCount, len(parts))
	}
	if got := parts[1].Files[0].Segments[0].Key; got != "full/data/src/events/all_1_1_0/checksums.txt" {
		t.Fatalf("deduplicated source = %q", got)
	}
	if parts[0].Bytes != 10 || len(parts[0].Files) != 2 {
		t.Fatalf("first part = %+v", parts[0])
	}
}

func TestPrepareResolvesIncrementalRenameAndPartialFile(t *testing.T) {
	base := writeIndex(t, `<config><version>1</version><uuid>base-uuid</uuid><contents>`+
		`<file><name>data/src/events/old_part/columns.txt</name><size>10</size><checksum>a</checksum></file>`+
		`<file><name>metadata/src/events.sql</name><size>50</size><checksum>b</checksum></file>`+
		`</contents></config>`)
	head := writeIndex(t, `<config><version>1</version><uuid>inc-uuid</uuid>`+
		`<base_backup>S3('s3://backups/base')</base_backup><base_backup_uuid>base-uuid</base_backup_uuid><contents>`+
		`<file><name>data/src/events/new_part/columns.txt</name><size>10</size><checksum>a</checksum><use_base>true</use_base></file>`+
		`<file><name>data/src/events/new_part/data.bin</name><size>15</size><checksum>c</checksum><use_base>true</use_base><base_size>10</base_size><base_checksum>a</base_checksum></file>`+
		`<file><name>metadata/src/events.sql</name><size>50</size><checksum>b</checksum><use_base>true</use_base></file>`+
		`</contents></config>`)
	plan, err := Prepare([]Layer{
		{Bucket: "backups", Prefix: "incremental", IndexPath: head},
		{Bucket: "backups", Prefix: "base", IndexPath: base},
	}, "src", "events")
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.Info().Metadata.Segments[0].Key; got != "base/metadata/src/events.sql" {
		t.Fatalf("metadata source = %q", got)
	}
	var part Part
	if err := plan.ScanParts(func(got Part) error { part = got; return nil }); err != nil {
		t.Fatal(err)
	}
	if got := part.Files[0].Segments; len(got) != 1 || got[0].Key != "base/data/src/events/old_part/columns.txt" {
		t.Fatalf("renamed file segments = %+v", got)
	}
	if got := part.Files[1].Segments; len(got) != 2 || got[0].Key != "base/data/src/events/old_part/columns.txt" || got[1].Key != "incremental/data/src/events/new_part/data.bin" {
		t.Fatalf("partial file segments = %+v", got)
	}
}

func TestPrepareRejectsUnsafePath(t *testing.T) {
	index := writeIndex(t, `<config><version>1</version><contents>`+
		`<file><name>data/src/events/all_1_1_0/../../escape</name><size>1</size><checksum>a</checksum></file>`+
		`</contents></config>`)
	_, err := Prepare([]Layer{{Bucket: "bucket", Prefix: "backup", IndexPath: index}}, "src", "events")
	if err == nil || !strings.Contains(err.Error(), "unsafe path") {
		t.Fatalf("error = %v", err)
	}
}

func TestReadHeader(t *testing.T) {
	header, err := ReadHeader(strings.NewReader(`<config><version>1</version><uuid>inc</uuid><base_backup>S3('s3://bucket/base')</base_backup><base_backup_uuid>base</base_backup_uuid><contents>`))
	if err != nil {
		t.Fatal(err)
	}
	if header.UUID != "inc" || header.BaseUUID != "base" || header.BaseBackup != "S3('s3://bucket/base')" {
		t.Fatalf("header = %+v", header)
	}
}

func TestEscapeForFileName(t *testing.T) {
	if got := EscapeForFileName("db.name/$table"); got != "db%2Ename%2F%24table" {
		t.Fatalf("escaped = %q", got)
	}
}

func writeIndex(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".backup")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
