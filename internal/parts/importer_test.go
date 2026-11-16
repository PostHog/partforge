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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PostHog/partforge/internal/artifact"
	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/s3copy"
)

func TestImportJobEmptyOutput(t *testing.T) {
	root := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "system.tables") {
			t.Errorf("unexpected query for empty output: %s", body)
		}
		fmt.Fprintln(w, root)
	}))
	defer server.Close()
	var transitions []string
	err := (Importer{ClickHouse: chhttp.Client{URL: server.URL}, WorkDir: t.TempDir()}).ImportJob(context.Background(), ImportJob{
		JobID: "job-1", Database: "db", Table: "dst", Parallelism: 1,
		Artifacts: []FinishedArtifact{{Bucket: "bucket", Key: "finished/part-1", PartID: "part-1", EmptyOutput: true}},
		MarkImporting: func(context.Context, FinishedArtifact) error {
			transitions = append(transitions, "importing")
			return nil
		},
		MarkImported: func(context.Context, FinishedArtifact) error {
			transitions = append(transitions, "imported")
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(transitions, ","); got != "importing,imported" {
		t.Fatalf("transitions = %s", got)
	}
}

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

	importer := Importer{
		S3Copy:     s3copy.Copier{Binary: binary},
		ClickHouse: chhttp.Client{URL: server.URL},
	}
	job := ImportJob{JobID: "job-1", Database: "db", Table: "dst"}
	extractRoot, partNames, err := importer.downloadArtifact(context.Background(), job, artifact, filepath.Join(root, "work"), owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := importer.attachParts(context.Background(), job, artifact, detachedPath, extractRoot, partNames); err != nil {
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
	childReadyFile := filepath.Join(dir, "child-ready")
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
	printf ready > %s
	sleep 3
) &
while [ ! -f %s ]; do sleep 0.01; done
printf ready > %s
wait
`, shellQuote(parentStoppedFile), shellQuote(childStoppedFile), shellQuote(childReadyFile), shellQuote(childReadyFile), shellQuote(readyFile))
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
			Artifacts:   []FinishedArtifact{{Bucket: "bucket", Key: "finished/part-1", PartID: "part-1"}},
			JobID:       "job-1",
			Database:    "db",
			Table:       "table",
			Parallelism: 1,
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

func TestImportJobCleansEachArtifactWorkDirectory(t *testing.T) {
	root := t.TempDir()
	dataPath := filepath.Join(root, "table")
	if err := os.Mkdir(dataPath, 0o755); err != nil {
		t.Fatal(err)
	}
	partRoot := filepath.Join(root, "source", "all_1_1_0")
	createPart(t, partRoot)
	tarPath := filepath.Join(root, "all_1_1_0.tar")
	if err := artifact.WriteFinishedTar(tarPath, []string{partRoot}); err != nil {
		t.Fatal(err)
	}
	binary, _ := fakeS5cmdDownload(t, tarPath)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if strings.Contains(string(body), "system.tables") {
			fmt.Fprintln(w, dataPath)
		} else if strings.Contains(string(body), "ATTACH PART") {
			if err := os.RemoveAll(filepath.Join(dataPath, "detached", "all_1_1_0")); err != nil {
				t.Error(err)
			}
		}
	}))
	defer server.Close()

	workDir := filepath.Join(root, "work")
	importsStarted := 0
	err := (Importer{
		S3Copy:     s3copy.Copier{Binary: binary},
		ClickHouse: chhttp.Client{URL: server.URL},
		WorkDir:    workDir,
	}).ImportJob(context.Background(), ImportJob{
		Artifacts: []FinishedArtifact{
			{Bucket: "bucket", Key: "finished/part-1", PartID: "part-1"},
			{Bucket: "bucket", Key: "finished/part-2", PartID: "part-2"},
		},
		JobID:       "job-1",
		Database:    "db",
		Table:       "table",
		Parallelism: 1,
		MarkImporting: func(context.Context, FinishedArtifact) error {
			importsStarted++
			if importsStarted == 2 {
				if _, err := os.Stat(filepath.Join(workDir, "job-1", "000000")); !os.IsNotExist(err) {
					return fmt.Errorf("first artifact work directory was not removed: %v", err)
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestImportJobParallelDownloadsSerializeAttach(t *testing.T) {
	root := t.TempDir()
	dataPath := filepath.Join(root, "table")
	if err := os.Mkdir(dataPath, 0o755); err != nil {
		t.Fatal(err)
	}
	partRoot := filepath.Join(root, "source", "all_1_1_0")
	createPart(t, partRoot)
	tarPath := filepath.Join(root, "all_1_1_0.tar")
	if err := artifact.WriteFinishedTar(tarPath, []string{partRoot}); err != nil {
		t.Fatal(err)
	}

	// Each fake download blocks until all three are running, so the import only
	// succeeds if downloads overlap.
	const parallelism = 3
	dir := t.TempDir()
	startedDir := filepath.Join(dir, "started")
	if err := os.Mkdir(startedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "s5cmd")
	script := fmt.Sprintf(`#!/bin/sh
dest=
for arg do dest=$arg; done
dest=${dest%%/}
mkdir -p "$dest"
touch %s/$$
i=0
while [ "$(ls %s | wc -l)" -lt %d ]; do
	i=$((i+1))
	if [ $i -gt 500 ]; then echo "downloads did not overlap" >&2; exit 1; fi
	sleep 0.01
done
cp %s "$dest/all_1_1_0.tar"
`, shellQuote(startedDir), shellQuote(startedDir), parallelism, shellQuote(tarPath))
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	var (
		inFlight    atomic.Int32
		maxInFlight atomic.Int32
		attaches    atomic.Int32
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if strings.Contains(string(body), "system.tables") {
			fmt.Fprintln(w, dataPath)
			return
		}
		if strings.Contains(string(body), "ATTACH PART") {
			n := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				prev := maxInFlight.Load()
				if n <= prev || maxInFlight.CompareAndSwap(prev, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			attaches.Add(1)
			if err := os.RemoveAll(filepath.Join(dataPath, "detached", "all_1_1_0")); err != nil {
				t.Error(err)
			}
		}
	}))
	defer server.Close()

	var mu sync.Mutex
	var imported []string
	err := (Importer{
		S3Copy:     s3copy.Copier{Binary: binary},
		ClickHouse: chhttp.Client{URL: server.URL},
		WorkDir:    filepath.Join(root, "work"),
	}).ImportJob(context.Background(), ImportJob{
		Artifacts: []FinishedArtifact{
			{Bucket: "bucket", Key: "finished/part-1", PartID: "part-1"},
			{Bucket: "bucket", Key: "finished/part-2", PartID: "part-2"},
			{Bucket: "bucket", Key: "finished/part-3", PartID: "part-3"},
		},
		JobID:       "job-1",
		Database:    "db",
		Table:       "table",
		Parallelism: parallelism,
		MarkImported: func(_ context.Context, a FinishedArtifact) error {
			mu.Lock()
			imported = append(imported, a.PartID)
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := attaches.Load(); got != parallelism {
		t.Fatalf("attaches = %d, want %d", got, parallelism)
	}
	if got := maxInFlight.Load(); got != 1 {
		t.Fatalf("max concurrent attaches = %d, want 1", got)
	}
	if len(imported) != parallelism {
		t.Fatalf("imported = %v", imported)
	}
}

func TestImportJobFailureReleasesInFlightSiblings(t *testing.T) {
	root := t.TempDir()
	dataPath := filepath.Join(root, "table")
	if err := os.Mkdir(dataPath, 0o755); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, dataPath)
	}))
	defer server.Close()

	// part-fail waits for part-slow to start downloading, then downloads
	// nothing so extraction fails without s5cmd retries. part-slow blocks
	// until it is terminated.
	dir := t.TempDir()
	slowReady := filepath.Join(dir, "slow-ready")
	binary := filepath.Join(dir, "s5cmd")
	script := fmt.Sprintf(`#!/bin/sh
dest=
for arg do dest=$arg; done
dest=${dest%%/}
mkdir -p "$dest"
case "$*" in
*part-fail*)
	while [ ! -f %s ]; do sleep 0.01; done
	exit 0
	;;
*)
	trap 'exit 1' TERM
	printf ready > %s
	sleep 5 &
	wait
	;;
esac
`, shellQuote(slowReady), shellQuote(slowReady))
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	failed := map[string]bool{}
	released := map[string]bool{}
	err := (Importer{
		S3Copy:     s3copy.Copier{Binary: binary},
		ClickHouse: chhttp.Client{URL: server.URL},
		WorkDir:    filepath.Join(root, "work"),
	}).ImportJob(context.Background(), ImportJob{
		Artifacts: []FinishedArtifact{
			{Bucket: "bucket", Key: "finished/part-fail", PartID: "part-fail"},
			{Bucket: "bucket", Key: "finished/part-slow", PartID: "part-slow"},
		},
		JobID:       "job-1",
		Database:    "db",
		Table:       "table",
		Parallelism: 2,
		ReleaseImport: func(_ context.Context, a FinishedArtifact) error {
			mu.Lock()
			released[a.PartID] = true
			mu.Unlock()
			return nil
		},
		MarkImportFailed: func(_ context.Context, a FinishedArtifact, _ error) error {
			mu.Lock()
			failed[a.PartID] = true
			mu.Unlock()
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "finished/part-fail contains no part tarballs") {
		t.Fatalf("error = %v, want part-fail extraction error", err)
	}
	if strings.Contains(err.Error(), "part-slow") {
		t.Fatalf("error = %v, should not report the released sibling", err)
	}
	if !failed["part-fail"] || failed["part-slow"] {
		t.Fatalf("failed = %v", failed)
	}
	if !released["part-slow"] || released["part-fail"] {
		t.Fatalf("released = %v", released)
	}
}

func TestImportJobRejectsZeroParallelism(t *testing.T) {
	err := (Importer{}).ImportJob(context.Background(), ImportJob{
		JobID:     "job-1",
		Artifacts: []FinishedArtifact{{Bucket: "bucket", Key: "finished/part-1", PartID: "part-1"}},
	})
	if err == nil || !strings.Contains(err.Error(), "parallelism") {
		t.Fatalf("error = %v, want parallelism error", err)
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
