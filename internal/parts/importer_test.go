package parts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PostHog/partforge/internal/artifact"
	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/s3copy"
)

func TestImportArtifactDownloadsFinishedTarballs(t *testing.T) {
	root := t.TempDir()
	partRoot := filepath.Join(root, "source", "all_1_1_0")
	createPart(t, partRoot)
	tarPath := filepath.Join(root, "all_1_1_0.tar")
	if err := artifact.WriteFinishedTar(tarPath, []string{partRoot}); err != nil {
		t.Fatal(err)
	}
	binary, logFile := fakeS5cmdDownload(t, tarPath)
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		queries = append(queries, string(body))
	}))
	defer server.Close()

	detachedPath := filepath.Join(root, "detached")
	if err := os.Mkdir(detachedPath, 0o755); err != nil {
		t.Fatal(err)
	}
	owner, err := ownerFromPath(detachedPath)
	if err != nil {
		t.Fatal(err)
	}
	artifact := FinishedArtifact{
		Bucket: "bucket",
		Key:    "partforge/jobs/job-1/finished/part-1",
		PartID: "part-1",
	}

	err = (Importer{
		S3Copy:     s3copy.Copier{Binary: binary},
		ClickHouse: chhttp.Client{URL: server.URL},
	}).importArtifact(context.Background(), ImportJob{
		JobID:    "job-1",
		Database: "db",
		Table:    "dst",
	}, artifact, detachedPath, filepath.Join(root, "work"), owner)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	call := strings.TrimSpace(string(raw))
	wantSource := "cp s3://bucket/" + artifact.Key + "/* "
	if !strings.Contains(call, wantSource) {
		t.Fatalf("download call = %q, want finished artifact prefix source %q", call, wantSource)
	}
	if strings.Contains(call, "/data/*") || strings.Contains(call, "/attempt-") {
		t.Fatalf("download call uses old finished layout: %q", call)
	}
	if len(queries) != 1 || !strings.Contains(queries[0], "ATTACH PART 'all_1_1_0'") {
		t.Fatalf("attach queries = %#v", queries)
	}
}

func TestImportJobCancellationCleansWorkAndReleasesArtifact(t *testing.T) {
	root := t.TempDir()
	dataPath := filepath.Join(root, "table")
	if err := os.Mkdir(dataPath, 0o755); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, dataPath)
	}))
	defer server.Close()

	dir := t.TempDir()
	binary := filepath.Join(dir, "s5cmd")
	readyFile := filepath.Join(dir, "ready")
	parentStoppedFile := filepath.Join(dir, "parent-stopped")
	childStoppedFile := filepath.Join(dir, "child-stopped")
	script := fmt.Sprintf(`#!/bin/sh
trap 'printf stopped > %s; wait; exit 0' TERM
dest=
for arg do dest=$arg; done
dest=${dest%%/}
mkdir -p "$dest"
printf partial > "$dest/partial.tar"
(
	trap 'printf stopped > %s; exit 0' TERM
	sleep 3
) &
printf ready > %s
wait
`, shellQuote(parentStoppedFile), shellQuote(childStoppedFile), shellQuote(readyFile))
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	workDir := filepath.Join(root, "work")
	released := false
	markedFailed := false
	done := make(chan error, 1)
	go func() {
		done <- (Importer{
			S3Copy:     s3copy.Copier{Binary: binary},
			ClickHouse: chhttp.Client{URL: server.URL},
			WorkDir:    workDir,
		}).ImportJob(ctx, ImportJob{
			Artifacts: []FinishedArtifact{{Bucket: "bucket", Key: "finished/part-1", PartID: "part-1"}},
			JobID:     "job-1",
			Database:  "db",
			Table:     "table",
			ReleaseImport: func(ctx context.Context, _ FinishedArtifact) error {
				released = ctx.Err() == nil
				return nil
			},
			MarkImportFailed: func(context.Context, FinishedArtifact, error) error {
				markedFailed = true
				return nil
			},
		})
	}()
	for deadline := time.Now().Add(2 * time.Second); ; {
		if _, err := os.Stat(readyFile); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fake s5cmd did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("import did not stop")
	}
	if !released || markedFailed {
		t.Fatalf("released = %t, marked failed = %t", released, markedFailed)
	}
	if _, err := os.Stat(filepath.Join(workDir, "job-1")); !os.IsNotExist(err) {
		t.Fatalf("import work directory was not removed: %v", err)
	}
	for _, path := range []string{parentStoppedFile, childStoppedFile} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("s5cmd process did not handle SIGTERM: %s", path)
		}
	}
}

func TestDefaultImportWorkDirUsesClickHouseDisk(t *testing.T) {
	got := defaultImportWorkDir("/var/lib/clickhouse/")
	want := filepath.Join("/var/lib/clickhouse", "partforge-import-work")
	if got != want {
		t.Fatalf("work dir = %q, want %q", got, want)
	}
}

func TestPathContains(t *testing.T) {
	if !pathContains("/var/lib/clickhouse/", "/var/lib/clickhouse/store/abc/table") {
		t.Fatal("expected child path to be contained")
	}
	if pathContains("/var/lib/clickhouse/store", "/var/lib/clickhouse/store-other/table") {
		t.Fatal("expected sibling prefix path not to be contained")
	}
}

func TestEnsureSameFilesystem(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	if err := os.Mkdir(a, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(b, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensureSameFilesystem(a, b); err != nil {
		t.Fatal(err)
	}
}

func fakeS5cmdDownload(t *testing.T, tarPath string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "s5cmd")
	logFile := filepath.Join(dir, "calls")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> " + shellQuote(logFile) + "\n" +
		"dest=\n" +
		"for arg do dest=$arg; done\n" +
		"dest=${dest%/}\n" +
		"mkdir -p \"$dest\"\n" +
		"cp " + shellQuote(tarPath) + " \"$dest/all_1_1_0.tar\"\n" +
		"exit 0\n"
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binary, logFile
}

func createPart(t *testing.T, root string) {
	t.Helper()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"checksums.txt", "columns.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "data.bin"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
