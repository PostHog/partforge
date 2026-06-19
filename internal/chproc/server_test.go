package chproc

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestArgsIncludeGeneratedStorageConfig(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "clickhouse")
	cfg := Config{
		Binary:     "clickhouse",
		ConfigFile: "/etc/clickhouse-server/config.xml",
		DataDir:    dataDir,
		Tuning:     Tuning{BackgroundPoolSize: 12},
	}

	got, err := cfg.args()
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"server",
		"--config-file=/etc/clickhouse-server/config.xml",
		"--log-file=" + filepath.Join(root, "logs", "clickhouse-server.log"),
		"--errorlog-file=" + filepath.Join(root, "logs", "clickhouse-server.err.log"),
		"--pid-file=" + filepath.Join(root, "clickhouse-server.pid"),
		"--",
		"--path=" + withTrailingSeparator(filepath.Join(root, "data")),
		"--tmp_path=" + withTrailingSeparator(filepath.Join(root, "tmp")),
		"--user_files_path=" + withTrailingSeparator(filepath.Join(root, "user_files")),
		"--format_schema_path=" + withTrailingSeparator(filepath.Join(root, "format_schemas")),
		"--custom_cached_disks_base_directory=" + withTrailingSeparator(filepath.Join(root, "caches")),
		"--filesystem_caches_path=" + withTrailingSeparator(filepath.Join(root, "filesystem_caches")),
		"--custom_local_disks_base_directory=" + withTrailingSeparator(filepath.Join(root, "disks")),
		"--user_directories.local_directory.path=" + withTrailingSeparator(filepath.Join(root, "access")),
		"--background_pool_size=12",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
	for _, path := range []string{"data", "tmp", "user_files", "format_schemas", "caches", "filesystem_caches", "disks", "access", "logs"} {
		if info, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Fatalf("stat generated path %s: %v", path, err)
		} else if !info.IsDir() {
			t.Fatalf("generated path %s is not a directory", path)
		}
	}
}

func TestArgsIncludeBackgroundPoolSize(t *testing.T) {
	cfg := Config{Binary: "clickhouse", Tuning: Tuning{BackgroundPoolSize: 12}}
	got, err := cfg.args()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"server",
		"--",
		"--background_pool_size=12",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestArgsForClickHouseServerBinaryOmitServerSubcommand(t *testing.T) {
	cfg := Config{Binary: "clickhouse-server", ConfigFile: "/etc/clickhouse-server/config.xml"}
	got, err := cfg.args()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--config-file=/etc/clickhouse-server/config.xml"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestArgsRejectInvalidBackgroundPoolSize(t *testing.T) {
	cfg := Config{Binary: "clickhouse", Tuning: Tuning{BackgroundPoolSize: -1}}
	_, err := cfg.args()
	if err == nil {
		t.Fatal("expected invalid background pool size error")
	}
}

func TestStartFailsWhenProcessExitsBeforeReady(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "fake-clickhouse")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 42\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(dir, "clickhouse")
	errorLogDir := filepath.Join(dataDir, "logs")
	if err := os.MkdirAll(errorLogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(errorLogDir, "clickhouse-server.err.log"), []byte("Code: 76. Cannot open file /tmp/clickhouse/status\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	server, err := Start(context.Background(), Config{
		DataDir: dataDir,
		Binary:  binary,
		URL:     "http://127.0.0.1:1",
		Timeout: time.Minute,
	})
	if server != nil {
		t.Fatalf("server = %+v, want nil", server)
	}
	if err == nil {
		t.Fatal("expected start error")
	}
	if !strings.Contains(err.Error(), "clickhouse server exited before becoming ready") {
		t.Fatalf("start error = %v", err)
	}
	if !strings.Contains(err.Error(), "Cannot open file /tmp/clickhouse/status") {
		t.Fatalf("start error missing ClickHouse log detail: %v", err)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatalf("Start did not fail fast: %s", time.Since(started))
	}
}

func TestClickHouseErrorLogLinePrefersErrorLine(t *testing.T) {
	got := clickHouseErrorLogLine("trace\n2026 <Error> Application: Code: 76. Cannot open file /tmp/status\n0. stack\n")
	want := "2026 <Error> Application: Code: 76. Cannot open file /tmp/status"
	if got != want {
		t.Fatalf("clickHouseErrorLogLine = %q, want %q", got, want)
	}
}
