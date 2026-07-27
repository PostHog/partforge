package rewrite

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PostHog/partforge/internal/artifact"
	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/metrics"
	"github.com/PostHog/partforge/internal/s3copy"
)

func TestCompactHandlesFragmentedInputDeadline(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		deadline             func() time.Time
		partsAfterFirstQuery uint64
		wantReduced          bool
		wantWait             bool
		checkWait            bool
		observerError        bool
		wantError            string
	}{
		{
			name:                 "deadline expires while waiting without progress",
			deadline:             func() time.Time { return time.Now().Add(time.Second) },
			partsAfterFirstQuery: 2,
			wantWait:             true,
			checkWait:            true,
		},
		{
			name:                 "expired deadline uploads existing reduction",
			deadline:             func() time.Time { return time.Now().Add(-time.Second) },
			partsAfterFirstQuery: 1,
			wantReduced:          true,
			checkWait:            true,
		},
		{
			name:                 "observer error remains fatal near deadline",
			deadline:             func() time.Time { return time.Now().Add(time.Second) },
			partsAfterFirstQuery: 2,
			observerError:        true,
			wantError:            "observe compact merge failures",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			dataPath := filepath.Join(root, "clickhouse", "store", "table")
			diskPath := filepath.Join(root, "clickhouse")
			inputDir := filepath.Join(root, "input")
			partDirs := []string{
				filepath.Join(root, "parts", "202601_1_1_0"),
				filepath.Join(root, "parts", "202601_2_2_0"),
			}
			for _, partDir := range partDirs {
				createFrozenPart(t, partDir)
			}
			if err := artifact.WriteFinishedTar(filepath.Join(inputDir, "input.tar"), partDirs); err != nil {
				t.Fatal(err)
			}
			binary, copyLog := fakeCompactS5cmd(t, inputDir)

			var partitionQueries atomic.Uint64
			var mergeCountQueries atomic.Uint64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				query := string(body)
				switch {
				case strings.HasPrefix(query, "SELECT arrayElement(data_paths"):
					_, _ = io.WriteString(w, dataPath+"\n")
				case strings.HasPrefix(query, "SELECT partition_id, count()"):
					parts := uint64(2)
					if partitionQueries.Add(1) > 1 {
						parts = tt.partsAfterFirstQuery
					}
					_, _ = io.WriteString(w, "202601\t"+strconv.FormatUint(parts, 10)+"\t20\t200\n")
				case strings.HasPrefix(query, "SELECT count() FROM system.merges"):
					mergeCountQueries.Add(1)
					_, _ = io.WriteString(w, "0\n")
				case strings.Contains(query, "FROM system.part_log"):
					if tt.observerError {
						http.Error(w, "merge observer broke", http.StatusInternalServerError)
					} else {
						_, _ = io.WriteString(w, "0\t0\t<none>\n")
					}
				case strings.HasPrefix(query, "SELECT partition_id, result_part_name"):
				case strings.HasPrefix(query, "SELECT name, path, type FROM system.disks"):
					_, _ = io.WriteString(w, "default\t"+diskPath+"\tlocal\n")
				case strings.Contains(query, " FREEZE WITH NAME '"):
					marker := " FREEZE WITH NAME '"
					freezeName := strings.TrimSuffix(query[strings.Index(query, marker)+len(marker):], "'")
					if err := writeCompactTestPart(filepath.Join(diskPath, "shadow", freezeName, "store", "abc", "def", "202601_1_2_1")); err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
					}
				case query == "SYSTEM FLUSH LOGS",
					strings.HasPrefix(query, "CREATE DATABASE "),
					strings.HasPrefix(query, "CREATE TABLE "),
					strings.HasPrefix(query, "SYSTEM STOP MERGES "),
					strings.HasPrefix(query, "SYSTEM START MERGES "),
					strings.HasPrefix(query, "ALTER TABLE "):
				default:
					http.Error(w, "unexpected query: "+query, http.StatusInternalServerError)
				}
			}))
			defer server.Close()

			result, err := (Compactor{
				S3Copy:             s3copy.Copier{Binary: binary},
				ClickHouse:         chhttp.Client{URL: server.URL},
				WorkDir:            filepath.Join(root, "work"),
				MergeSettleMinWait: time.Second,
				MergePollInterval:  time.Millisecond,
				MergeDeadline:      tt.deadline(),
				MergeTreeSettings: MergeTreeSettings{
					MergeMaxBlockSize:        32768,
					MergeMaxBlockSizeBytes:   10 * 1024 * 1024,
					MergeSelectingSleepMS:    1000,
					PoolFreeEntriesThreshold: 1,
					DefaultCompressionCodec:  "ZSTD(5)",
				},
			}).Compact(context.Background(), CompactWorkItem{
				JobID:               "job-1",
				OutputPartID:        "compact-1",
				OutputFinishedKey:   "finished/compact-1",
				DestinationDatabase: "db",
				DestinationTable:    "events",
				DestinationSchema:   "CREATE TABLE source.events (x UInt64) ENGINE = MergeTree ORDER BY x",
				Inputs: []CompactInput{{
					PartID:          "part-1",
					Bucket:          "bucket",
					FinishedKey:     "finished/part-1",
					Parts:           2,
					Rows:            20,
					Bytes:           200,
					PartitionCounts: map[string]uint64{"202601": 2},
				}},
			})
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("error = %v, want %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Reduced != tt.wantReduced {
				t.Fatalf("reduced = %t, want %t", result.Reduced, tt.wantReduced)
			}
			if (result.FinishedKey == "finished/compact-1") != tt.wantReduced {
				t.Fatalf("finished key = %q, want uploaded %t", result.FinishedKey, tt.wantReduced)
			}
			if tt.checkWait && (mergeCountQueries.Load() > 0) != tt.wantWait {
				t.Fatalf("merge count queries = %d, want wait %t", mergeCountQueries.Load(), tt.wantWait)
			}
			calls, err := os.ReadFile(copyLog)
			if err != nil {
				t.Fatal(err)
			}
			uploaded := false
			for _, call := range strings.Split(string(calls), "\n") {
				if strings.Contains(call, " cp ") && strings.HasSuffix(call, " s3://bucket/finished/compact-1/") {
					uploaded = true
				}
			}
			if uploaded != tt.wantReduced {
				t.Fatalf("s5cmd calls = %q, uploaded = %t, want %t", calls, uploaded, tt.wantReduced)
			}
		})
	}
}

func fakeCompactS5cmd(t *testing.T, inputDir string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "s5cmd")
	logFile := filepath.Join(dir, "calls")
	script := "#!/bin/sh\n" +
		"last=''\n" +
		"for arg in \"$@\"; do last=\"$arg\"; done\n" +
		"printf '%s\\n' \"$*\" >> " + shellQuote(logFile) + "\n" +
		"case \"$last\" in\n" +
		"  s3://*) exit 0 ;;\n" +
		"  *) cp " + shellQuote(inputDir) + "/*.tar \"$last\" ;;\n" +
		"esac\n"
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binary, logFile
}

func writeCompactTestPart(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, name := range []string{"checksums.txt", "columns.txt", "data.bin"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("data"), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func TestConfigureCompactMergeSettingsAppliesMemorySafeMergeSettings(t *testing.T) {
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

	err := (Compactor{
		ClickHouse: chhttp.Client{URL: server.URL},
		MergeTreeSettings: MergeTreeSettings{
			MergeMaxBlockSize:        32768,
			MergeMaxBlockSizeBytes:   10 * 1024 * 1024,
			MergeSelectingSleepMS:    1000,
			PoolFreeEntriesThreshold: 1,
		},
	}).configureCompactMergeSettings(context.Background(), CompactWorkItem{
		JobID:               "job-1",
		OutputPartID:        "compact-1",
		DestinationDatabase: "db",
		DestinationTable:    "query_log_archive_temp",
	}, 100*1024*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) != 1 {
		t.Fatalf("queries = %#v, want one query", queries)
	}
	for _, setting := range []string{
		"merge_max_block_size_bytes = 10485760",
		"max_parts_to_merge_at_once = 100",
		"min_age_to_force_merge_seconds = 1",
		"min_age_to_force_merge_on_partition_only = 0",
		"enable_vertical_merge_algorithm = 1",
		"allow_vertical_merges_from_compact_to_wide_parts = 1",
		"vertical_merge_algorithm_min_rows_to_activate = 0",
		"vertical_merge_algorithm_min_bytes_to_activate = 0",
		"vertical_merge_algorithm_min_columns_to_activate = 0",
	} {
		if !strings.Contains(queries[0], setting) {
			t.Fatalf("query = %q, want %s", queries[0], setting)
		}
	}
}

func TestCompactProgressRejectsOutputMoreThanAttachedInput(t *testing.T) {
	err := (Compactor{}).reportProgress(context.Background(), CompactWorkItem{
		JobID:        "job-1",
		OutputPartID: "compact-1",
	}, CompactProgressSnapshot{
		InputStats:       metrics.PartStats{Count: 2},
		DestinationStats: metrics.PartStats{Count: 3},
	})
	if err == nil {
		t.Fatal("expected compact part accounting error")
	}
	if !strings.Contains(err.Error(), "exceeds attached input parts") {
		t.Fatalf("error = %v, want attached input accounting error", err)
	}
}

func TestCompactInputNeedsNormalization(t *testing.T) {
	if !compactInputNeedsNormalization([]CompactInput{{
		Parts:           3,
		PartitionCounts: map[string]uint64{"202606": 2, "202607": 1},
	}}) {
		t.Fatal("expected fragmented single input to need normalization")
	}
	if compactInputNeedsNormalization([]CompactInput{{
		Parts:           2,
		PartitionCounts: map[string]uint64{"202606": 1, "202607": 1},
	}}) {
		t.Fatal("expected one part per partition to be normalized")
	}
	if compactInputNeedsNormalization([]CompactInput{{PartitionCounts: map[string]uint64{"202606": 2}}, {PartitionCounts: map[string]uint64{"202606": 1}}}) {
		t.Fatal("expected multi-input compaction not to use normalization path")
	}
}

func TestNormalizeCompactInputUsesBackgroundMerges(t *testing.T) {
	partitionQueries := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		query := string(body)
		switch {
		case strings.HasPrefix(query, "SELECT count() FROM system.merges"):
			_, _ = io.WriteString(w, "0\n")
		case strings.HasPrefix(query, "SELECT partition_id, count()"):
			partitionQueries++
			parts := "2"
			if partitionQueries > 1 {
				parts = "1"
			}
			_, _ = io.WriteString(w, "202606\t"+parts+"\t100\t1000\n")
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()

	p := Processor{ClickHouse: chhttp.Client{URL: server.URL}, MergePollInterval: time.Millisecond}
	err := (Compactor{}).normalizeCompactInput(
		context.Background(),
		p,
		CompactWorkItem{JobID: "job-1", OutputPartID: "compact-1"},
		mergeWaitTarget{Database: "db", Table: "events"},
		[]PartPartitionStats{{PartitionID: "202606", Parts: 2}},
		metrics.PartStats{Count: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	if partitionQueries != 2 {
		t.Fatalf("partition queries = %d, want 2", partitionQueries)
	}
}

func TestNormalizeCompactInputCheckpointsAfterProgressStops(t *testing.T) {
	partitionQueries := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		query := string(body)
		switch {
		case strings.HasPrefix(query, "SELECT count() FROM system.merges"):
			_, _ = io.WriteString(w, "0\n")
		case strings.HasPrefix(query, "SELECT partition_id, count()"):
			partitionQueries++
			_, _ = io.WriteString(w, "202606\t128\t100\t1000\n")
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()

	p := Processor{
		ClickHouse:         chhttp.Client{URL: server.URL},
		MergePollInterval:  time.Millisecond,
		MergeSettleMinWait: time.Millisecond,
	}
	err := (Compactor{ClickHouse: p.ClickHouse}).normalizeCompactInput(
		context.Background(),
		p,
		CompactWorkItem{JobID: "job-1", OutputPartID: "compact-1"},
		mergeWaitTarget{Database: "db", Table: "events"},
		[]PartPartitionStats{{PartitionID: "202606", Parts: 129}},
		metrics.PartStats{Count: 129},
	)
	if err != nil {
		t.Fatal(err)
	}
	if partitionQueries < 2 {
		t.Fatalf("partition queries = %d, want at least 2", partitionQueries)
	}
}

func TestNormalizeCompactInputFailsWithoutProgress(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		query := string(body)
		switch {
		case strings.HasPrefix(query, "SELECT count() FROM system.merges"):
			_, _ = io.WriteString(w, "0\n")
		case strings.HasPrefix(query, "SELECT partition_id, count()"):
			_, _ = io.WriteString(w, "202606\t129\t100\t1000\n")
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()

	p := Processor{
		ClickHouse:         chhttp.Client{URL: server.URL},
		MergePollInterval:  time.Millisecond,
		MergeSettleMinWait: time.Millisecond,
	}
	err := (Compactor{}).normalizeCompactInput(
		context.Background(),
		p,
		CompactWorkItem{JobID: "job-1", OutputPartID: "compact-1"},
		mergeWaitTarget{Database: "db", Table: "events"},
		[]PartPartitionStats{{PartitionID: "202606", Parts: 129}},
		metrics.PartStats{Count: 129},
	)
	if err == nil || !strings.Contains(err.Error(), "made no progress") {
		t.Fatalf("error = %v, want no-progress failure", err)
	}
}

func TestNormalizeCompactInputKeepsQuietTimerAliveDuringMerge(t *testing.T) {
	var mergeQueries atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.HasPrefix(query, "SELECT count() FROM system.merges"):
			count := mergeQueries.Add(1)
			if count <= 10 {
				_, _ = io.WriteString(w, "1\n")
			} else {
				_, _ = io.WriteString(w, "0\n")
			}
		case strings.HasPrefix(query, "SELECT partition_id, count()"):
			parts := "129"
			if mergeQueries.Load() > 11 {
				parts = "128"
			}
			_, _ = io.WriteString(w, "202606\t"+parts+"\t100\t1000\n")
		default:
			http.Error(w, "unexpected query: "+query, http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	p := Processor{
		ClickHouse:         chhttp.Client{URL: server.URL},
		MergePollInterval:  time.Millisecond,
		MergeSettleMinWait: 5 * time.Millisecond,
	}
	err := (Compactor{}).normalizeCompactInput(
		context.Background(),
		p,
		CompactWorkItem{JobID: "job-1", OutputPartID: "compact-1"},
		mergeWaitTarget{Database: "db", Table: "events"},
		[]PartPartitionStats{{PartitionID: "202606", Parts: 129}},
		metrics.PartStats{Count: 129},
	)
	if err != nil {
		t.Fatal(err)
	}
	if mergeQueries.Load() <= 11 {
		t.Fatalf("merge queries = %d, expected merge completion and later part reduction", mergeQueries.Load())
	}
}

func TestCompactPartitionsNormalizedRequiresSameSinglePartPartitions(t *testing.T) {
	input := []PartPartitionStats{
		{PartitionID: "202601", Parts: 4},
		{PartitionID: "202602", Parts: 3},
	}
	for _, tt := range []struct {
		name   string
		output []PartPartitionStats
		want   bool
	}{
		{name: "same partitions", output: []PartPartitionStats{{PartitionID: "202602", Parts: 1}, {PartitionID: "202601", Parts: 1}}, want: true},
		{name: "fragment remains", output: []PartPartitionStats{{PartitionID: "202601", Parts: 2}, {PartitionID: "202602", Parts: 1}}},
		{name: "partition missing", output: []PartPartitionStats{{PartitionID: "202601", Parts: 1}}},
		{name: "partition replaced", output: []PartPartitionStats{{PartitionID: "202601", Parts: 1}, {PartitionID: "202603", Parts: 1}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := compactPartitionsNormalized(input, tt.output); got != tt.want {
				t.Fatalf("compactPartitionsNormalized() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestCompactorPhaseContextCancelsOnShutdown(t *testing.T) {
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	phaseCtx, cancelPhase := (Compactor{ShutdownContext: shutdownCtx}).phaseContext(context.Background())
	defer cancelPhase()

	cancelShutdown()

	select {
	case <-phaseCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("phase context did not cancel after shutdown")
	}
	if !errors.Is(phaseCtx.Err(), context.Canceled) {
		t.Fatalf("phase context error = %v, want context.Canceled", phaseCtx.Err())
	}
}

func TestCompactMergeTimeoutUntil(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)

	timeout, ok := compactMergeTimeoutUntil(time.Time{}, now)
	if ok || timeout != 0 {
		t.Fatalf("compactMergeTimeoutUntil without deadline = %s, %t; want 0, false", timeout, ok)
	}

	timeout, ok = compactMergeTimeoutUntil(now.Add(30*time.Minute), now)
	if !ok || timeout != 30*time.Minute {
		t.Fatalf("compactMergeTimeoutUntil future = %s, %t; want 30m, true", timeout, ok)
	}

	timeout, ok = compactMergeTimeoutUntil(now, now)
	if !ok || timeout != 0 {
		t.Fatalf("compactMergeTimeoutUntil elapsed = %s, %t; want 0, true", timeout, ok)
	}
}

func TestFragmentedCompactWaitDeadlineIsNotFatal(t *testing.T) {
	if fragmentedCompactWaitIsFatal(true, true, context.DeadlineExceeded) {
		t.Fatal("expected fragmented compaction deadline to continue to output measurement")
	}
	if !fragmentedCompactWaitIsFatal(true, false, context.DeadlineExceeded) {
		t.Fatal("expected an unrelated fragmented compaction deadline to remain fatal")
	}
	if !fragmentedCompactWaitIsFatal(true, true, errors.New("merge failed")) {
		t.Fatal("expected a real fragmented compaction error to remain fatal")
	}
}

func TestCompactMergeTimeoutsForDeadlineKeepsIdleTimeout(t *testing.T) {
	timeout, maxTimeout := compactMergeTimeoutsForDeadline(15*time.Minute, 24*time.Hour, 2*time.Hour)

	if timeout != 15*time.Minute {
		t.Fatalf("timeout = %s, want compact idle timeout", timeout)
	}
	if maxTimeout != 2*time.Hour {
		t.Fatalf("max timeout = %s, want compact window deadline", maxTimeout)
	}
}

func TestCompactMergeTimeoutsForDeadlineCapsIdleTimeout(t *testing.T) {
	timeout, maxTimeout := compactMergeTimeoutsForDeadline(15*time.Minute, 24*time.Hour, time.Minute)

	if timeout != time.Minute {
		t.Fatalf("timeout = %s, want remaining deadline", timeout)
	}
	if maxTimeout != time.Minute {
		t.Fatalf("max timeout = %s, want remaining deadline", maxTimeout)
	}
}

func TestAddPartStats(t *testing.T) {
	got := addPartStats(
		metrics.PartStats{Count: 1, Rows: 2, Bytes: 3},
		metrics.PartStats{Count: 4, Rows: 5, Bytes: 6},
	)
	want := metrics.PartStats{Count: 5, Rows: 7, Bytes: 9}
	if got != want {
		t.Fatalf("addPartStats = %+v, want %+v", got, want)
	}
}
