package s3copy

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	s5cmdRetryBaseDelay = 0
	os.Exit(m.Run())
}

func TestCopyArgsUseSharedS5cmdRetries(t *testing.T) {
	copier := Copier{Endpoint: "http://localhost:4566", NumWorkers: 64}
	got := copier.copyArgs("/tmp/source/", "s3://bucket/prefix/")
	want := []string{
		"--log=error",
		"--retry-count", "3",
		"--numworkers", "64",
		"--endpoint-url", "http://localhost:4566",
		"cp",
		"/tmp/source/",
		"s3://bucket/prefix/",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestDeleteArgsUseSharedS5cmdRetries(t *testing.T) {
	copier := Copier{Endpoint: "http://localhost:4566", NumWorkers: 64}
	got := copier.deleteArgs("s3://bucket/prefix/*")
	want := []string{
		"--log=error",
		"--retry-count", "3",
		"--numworkers", "64",
		"--endpoint-url", "http://localhost:4566",
		"rm",
		"s3://bucket/prefix/*",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestCopyArgsOmitsNumWorkersWhenUnset(t *testing.T) {
	copier := Copier{}
	got := copier.copyArgs("/tmp/source/", "s3://bucket/prefix/")
	want := []string{
		"--log=error",
		"--retry-count", "3",
		"cp",
		"/tmp/source/",
		"s3://bucket/prefix/",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestUploadDirRetriesCopyCommand(t *testing.T) {
	binary, attemptsFile := fakeS5cmd(t, 3)
	localDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(localDir, "data.bin"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	copier := Copier{Binary: binary}
	if err := copier.UploadDir(context.Background(), localDir, "bucket", "prefix"); err != nil {
		t.Fatal(err)
	}
	if got := readAttemptCount(t, attemptsFile); got != 4 {
		t.Fatalf("attempts = %d, want 4", got)
	}
}

func TestDownloadPrefixRetriesCopyCommand(t *testing.T) {
	binary, attemptsFile := fakeS5cmd(t, 3)
	localDir := filepath.Join(t.TempDir(), "download")

	copier := Copier{Binary: binary}
	if err := copier.DownloadPrefix(context.Background(), "bucket", "prefix", localDir); err != nil {
		t.Fatal(err)
	}
	if got := readAttemptCount(t, attemptsFile); got != 4 {
		t.Fatalf("attempts = %d, want 4", got)
	}
}

func TestDownloadFileRetriesCopyCommand(t *testing.T) {
	binary, attemptsFile := fakeS5cmd(t, 3)
	localPath := filepath.Join(t.TempDir(), "download", "manifest.json")

	copier := Copier{Binary: binary}
	if err := copier.DownloadFile(context.Background(), "bucket", "prefix/manifest.json", localPath); err != nil {
		t.Fatal(err)
	}
	if got := readAttemptCount(t, attemptsFile); got != 4 {
		t.Fatalf("attempts = %d, want 4", got)
	}
}

func TestUploadGlobRetriesCopyCommand(t *testing.T) {
	binary, attemptsFile := fakeS5cmd(t, 3)

	copier := Copier{Binary: binary}
	if err := copier.UploadGlob(context.Background(), "/tmp/source/*/*/*", "bucket", "prefix"); err != nil {
		t.Fatal(err)
	}
	if got := readAttemptCount(t, attemptsFile); got != 4 {
		t.Fatalf("attempts = %d, want 4", got)
	}
}

func TestCopyObjectsRunsBatchFromStdin(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "s5cmd")
	argsFile := filepath.Join(dir, "args")
	stdinFile := filepath.Join(dir, "stdin")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s' \"$*\" > %s\ncat > %s\n", shellQuote(argsFile), shellQuote(stdinFile))
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	copier := Copier{Binary: binary, Endpoint: "http://localhost:4566", NumWorkers: 8}
	err := copier.CopyObjects(context.Background(), []ObjectCopy{
		{SourceBucket: "backups", SourceKey: "full/data/a.bin", DestinationBucket: "partforge", DestinationKey: "jobs/job/source/a.bin"},
		{SourceBucket: "backups", SourceKey: "full/data/file with space.bin", DestinationBucket: "partforge", DestinationKey: "jobs/job/source/file with space.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(args), "--log=error --retry-count 3 --numworkers 8 --endpoint-url http://localhost:4566 run"; got != want {
		t.Fatalf("args = %q, want %q", got, want)
	}
	stdin, err := os.ReadFile(stdinFile)
	if err != nil {
		t.Fatal(err)
	}
	want := "cp --raw \"s3://backups/full/data/a.bin\" \"s3://partforge/jobs/job/source/a.bin\"\n" +
		"cp --raw \"s3://backups/full/data/file with space.bin\" \"s3://partforge/jobs/job/source/file with space.bin\"\n"
	if string(stdin) != want {
		t.Fatalf("stdin = %q, want %q", stdin, want)
	}
}

func TestDownloadObjectsRunsBatchFromStdin(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "s5cmd")
	stdinFile := filepath.Join(dir, "stdin")
	script := fmt.Sprintf("#!/bin/sh\ncat > %s\n", shellQuote(stdinFile))
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	destination := filepath.Join(dir, "part", "data.bin")
	err := (Copier{Binary: binary}).DownloadObjects(context.Background(), []ObjectDownload{{
		SourceBucket: "backups", SourceKey: "full/data.bin", DestinationPath: destination,
	}})
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := os.ReadFile(stdinFile)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("cp --raw \"s3://backups/full/data.bin\" %q\n", destination)
	if string(stdin) != want {
		t.Fatalf("stdin = %q, want %q", stdin, want)
	}
}

func TestUploadDirFailsAfterCopyRetries(t *testing.T) {
	binary, attemptsFile := fakeS5cmd(t, 10)
	localDir := t.TempDir()

	copier := Copier{Binary: binary}
	err := copier.UploadDir(context.Background(), localDir, "bucket", "prefix")
	if err == nil {
		t.Fatal("expected upload error")
	}
	if got := readAttemptCount(t, attemptsFile); got != 4 {
		t.Fatalf("attempts = %d, want 4", got)
	}
	if !strings.Contains(err.Error(), "s5cmd command failed after 4 attempts") {
		t.Fatalf("error = %q, want exhausted retry message", err)
	}
}

func TestDeletePrefixRetriesCommand(t *testing.T) {
	binary, attemptsFile := fakeS5cmd(t, 10)

	copier := Copier{Binary: binary}
	err := copier.DeletePrefix(context.Background(), "bucket", "prefix")
	if err == nil {
		t.Fatal("expected delete error")
	}
	if got := readAttemptCount(t, attemptsFile); got != 4 {
		t.Fatalf("attempts = %d, want 4", got)
	}
	if !strings.Contains(err.Error(), "s5cmd command failed after 4 attempts") {
		t.Fatalf("error = %q, want exhausted retry message", err)
	}
}

func TestDeletePrefixIfExistsIgnoresNoObjectFound(t *testing.T) {
	binary, attemptsFile := fakeS5cmdOutputWithAttempts(t, `ERROR "rm s3://bucket/prefix/*": no object found`, 1)

	copier := Copier{Binary: binary}
	if err := copier.DeletePrefixIfExists(context.Background(), "bucket", "prefix"); err != nil {
		t.Fatal(err)
	}
	if got := readAttemptCount(t, attemptsFile); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestDeletePrefixIfExistsFailsOtherDeleteErrors(t *testing.T) {
	binary := fakeS5cmdOutput(t, "access denied", 1)

	copier := Copier{Binary: binary}
	err := copier.DeletePrefixIfExists(context.Background(), "bucket", "prefix")
	if err == nil {
		t.Fatal("expected delete error")
	}
	if !strings.Contains(err.Error(), "access denied") {
		t.Fatalf("error = %q, want access denied", err)
	}
}

func TestDeletePrefixTarget(t *testing.T) {
	got, err := deletePrefixTarget("bucket", "partforge/jobs/job-123")
	if err != nil {
		t.Fatal(err)
	}
	want := "s3://bucket/partforge/jobs/job-123/*"
	if got != want {
		t.Fatalf("target = %q, want %q", got, want)
	}
}

func TestDeletePrefixTargetRejectsGlobMeta(t *testing.T) {
	if _, err := deletePrefixTarget("bucket", "partforge/jobs/job-*"); err == nil {
		t.Fatal("expected glob metacharacter error")
	}
}

func TestS5cmdCommandRetryDelayBacksOff(t *testing.T) {
	old := s5cmdRetryBaseDelay
	s5cmdRetryBaseDelay = time.Second
	t.Cleanup(func() {
		s5cmdRetryBaseDelay = old
	})

	for attempt, want := range map[int]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
	} {
		if got := s5cmdCommandRetryDelay(attempt); got != want {
			t.Fatalf("retry delay for attempt %d = %s, want %s", attempt, got, want)
		}
	}
}

func fakeS5cmd(t *testing.T, failUntil int) (string, string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "s5cmd")
	attemptsFile := filepath.Join(dir, "attempts")
	script := fmt.Sprintf(`#!/bin/sh
count_file=%s
count=0
if [ -f "$count_file" ]; then
	count=$(cat "$count_file")
fi
count=$((count + 1))
printf '%%s' "$count" > "$count_file"
if [ "$count" -le %d ]; then
	echo "fake failure $count" >&2
	exit 42
fi
exit 0
`, shellQuote(attemptsFile), failUntil)
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binary, attemptsFile
}

func fakeS5cmdOutput(t *testing.T, output string, exitCode int) string {
	binary, _ := fakeS5cmdOutputWithAttempts(t, output, exitCode)
	return binary
}

func fakeS5cmdOutputWithAttempts(t *testing.T, output string, exitCode int) (string, string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "s5cmd")
	attemptsFile := filepath.Join(dir, "attempts")
	script := fmt.Sprintf(`#!/bin/sh
count_file=%s
count=0
if [ -f "$count_file" ]; then
	count=$(cat "$count_file")
fi
count=$((count + 1))
printf '%%s' "$count" > "$count_file"
echo %s >&2
exit %d
`, shellQuote(attemptsFile), shellQuote(output), exitCode)
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binary, attemptsFile
}

func readAttemptCount(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	count, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
