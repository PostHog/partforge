package rewrite

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/metrics"
)

func TestConfigureCompactMergeSettingsLeavesDefaultSettingsUntouched(t *testing.T) {
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
	for _, setting := range []string{"enable_vertical_merge_algorithm", "merge_max_block_size_bytes"} {
		if strings.Contains(queries[0], setting) {
			t.Fatalf("query = %q, want %s unset", queries[0], setting)
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

func TestNormalizeCompactInputVerifiesPartsWhileOptimizeRuns(t *testing.T) {
	type optimizeRequest struct {
		query   string
		queryID string
	}
	requests := make(chan optimizeRequest, 1)
	optimizeStarted := make(chan struct{})
	var partitionQueries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.HasPrefix(query, "OPTIMIZE TABLE"):
			requests <- optimizeRequest{query: query, queryID: r.URL.Query().Get("query_id")}
			close(optimizeStarted)
			<-r.Context().Done()
		case strings.HasPrefix(query, "SELECT count() FROM system.merges"):
			_, _ = io.WriteString(w, "0\n")
		case strings.HasPrefix(query, "SELECT partition_id, count()"):
			<-optimizeStarted
			queryNumber := partitionQueries.Add(1)
			parts := "2"
			if queryNumber > 1 {
				parts = "1"
			}
			_, _ = io.WriteString(w, "202606\t"+parts+"\t100\t1000\n")
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	item := CompactWorkItem{JobID: "job-1", OutputPartID: "compact-1"}
	target := mergeWaitTarget{Database: "db", Table: "events"}
	p := Processor{ClickHouse: chhttp.Client{URL: server.URL}, MergePollInterval: time.Millisecond}
	err := (Compactor{ClickHouse: p.ClickHouse}).normalizeCompactInput(context.Background(), p, item, target, []PartPartitionStats{{PartitionID: "202606", Parts: 2}}, metrics.PartStats{Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if request.query != "OPTIMIZE TABLE `db`.`events` PARTITION ID '202606' FINAL SETTINGS optimize_throw_if_noop = 1" {
		t.Fatalf("optimize query = %q", request.query)
	}
	if request.queryID != "partforge-job-1-compact-1-optimize-final-attempt-1" {
		t.Fatalf("optimize query ID = %q", request.queryID)
	}
	if partitionQueries.Load() != 2 {
		t.Fatalf("partition verification queries = %d, want 2", partitionQueries.Load())
	}
}

func TestNormalizeCompactInputRepeatsOptimizeAfterProgress(t *testing.T) {
	optimizeStarted := make(chan struct{})
	var optimizeAttempts atomic.Int32
	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.HasPrefix(query, "OPTIMIZE TABLE"):
			requests <- query
			if optimizeAttempts.Add(1) == 1 {
				close(optimizeStarted)
			}
			w.WriteHeader(http.StatusInternalServerError)
		case strings.HasPrefix(query, "SELECT count() FROM system.merges"):
			_, _ = io.WriteString(w, "0\n")
		case strings.HasPrefix(query, "SELECT partition_id, count()"):
			<-optimizeStarted
			parts := "202606\t1\t50\t500\n202607\t2\t50\t500\n"
			if optimizeAttempts.Load() > 1 {
				parts = "202606\t1\t50\t500\n202607\t1\t50\t500\n"
			}
			_, _ = io.WriteString(w, parts)
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	item := CompactWorkItem{JobID: "job-1", OutputPartID: "compact-1"}
	target := mergeWaitTarget{Database: "db", Table: "events"}
	p := Processor{ClickHouse: chhttp.Client{URL: server.URL}, MergePollInterval: time.Millisecond}
	err := (Compactor{ClickHouse: p.ClickHouse}).normalizeCompactInput(context.Background(), p, item, target, []PartPartitionStats{{PartitionID: "202606", Parts: 2}, {PartitionID: "202607", Parts: 2}}, metrics.PartStats{Count: 4})
	if err != nil {
		t.Fatal(err)
	}
	if optimizeAttempts.Load() != 2 {
		t.Fatalf("optimize attempts = %d, want 2", optimizeAttempts.Load())
	}
	for i, want := range []string{
		"OPTIMIZE TABLE `db`.`events` PARTITION ID '202606' FINAL SETTINGS optimize_throw_if_noop = 1",
		"OPTIMIZE TABLE `db`.`events` PARTITION ID '202607' FINAL SETTINGS optimize_throw_if_noop = 1",
	} {
		if got := <-requests; got != want {
			t.Fatalf("optimize request %d = %q, want %q", i+1, got, want)
		}
	}
}

func TestNormalizeCompactInputRetriesMemoryLimitedObservation(t *testing.T) {
	var mergeQueries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.HasPrefix(query, "OPTIMIZE TABLE"):
		case strings.HasPrefix(query, "SELECT count() FROM system.merges"):
			if mergeQueries.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, "Code: 241. DB::Exception: MEMORY_LIMIT_EXCEEDED")
				return
			}
			_, _ = io.WriteString(w, "0\n")
		case strings.HasPrefix(query, "SELECT partition_id, count()"):
			_, _ = io.WriteString(w, "202606\t1\t100\t1000\n")
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	item := CompactWorkItem{JobID: "job-1", OutputPartID: "compact-1"}
	target := mergeWaitTarget{Database: "db", Table: "events"}
	p := Processor{ClickHouse: chhttp.Client{URL: server.URL}, MergePollInterval: time.Millisecond}
	err := (Compactor{ClickHouse: p.ClickHouse}).normalizeCompactInput(context.Background(), p, item, target, []PartPartitionStats{{PartitionID: "202606", Parts: 2}}, metrics.PartStats{Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	if mergeQueries.Load() != 2 {
		t.Fatalf("merge observation queries = %d, want 2", mergeQueries.Load())
	}
}

func TestNormalizeCompactInputFailsWhenOptimizeMakesNoProgress(t *testing.T) {
	optimizeStarted := make(chan struct{})
	optimizeCanceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.HasPrefix(query, "OPTIMIZE TABLE"):
			close(optimizeStarted)
			<-r.Context().Done()
			close(optimizeCanceled)
		case strings.HasPrefix(query, "SELECT count() FROM system.merges"):
			_, _ = io.WriteString(w, "0\n")
		case strings.HasPrefix(query, "SELECT partition_id, count()"):
			<-optimizeStarted
			_, _ = io.WriteString(w, "202606\t2\t100\t1000\n")
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	item := CompactWorkItem{JobID: "job-1", OutputPartID: "compact-1"}
	target := mergeWaitTarget{Database: "db", Table: "events"}
	p := Processor{
		ClickHouse:         chhttp.Client{URL: server.URL},
		MergePollInterval:  time.Millisecond,
		MergeSettleMinWait: 10 * time.Millisecond,
	}
	err := (Compactor{ClickHouse: p.ClickHouse}).normalizeCompactInput(context.Background(), p, item, target, []PartPartitionStats{{PartitionID: "202606", Parts: 2}}, metrics.PartStats{Count: 2})
	if err == nil || !strings.Contains(err.Error(), "made no progress") {
		t.Fatalf("error = %v, want no-progress error", err)
	}
	select {
	case <-optimizeCanceled:
	case <-time.After(time.Second):
		t.Fatal("OPTIMIZE request was not canceled")
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
