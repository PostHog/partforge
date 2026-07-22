package rewrite

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/metrics"
)

func TestObserveCompactProgressFailsAfterThreeNonMemoryMergeFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		query := string(body)
		switch {
		case query == "SYSTEM FLUSH LOGS":
		case strings.Contains(query, "FROM system.part_log"):
			_, _ = io.WriteString(w, "3\t0\tCode: 999. DB::Exception: merge failed\n")
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()

	p := Processor{ClickHouse: chhttp.Client{URL: server.URL}}
	err := (Compactor{}).observeCompactProgress(context.Background(), p, CompactWorkItem{
		JobID:        "job-1",
		OutputPartID: "compact-1",
	}, mergeWaitTarget{Database: "db", Table: "events"}, metrics.PartStats{Count: 10})
	if err == nil || !strings.Contains(err.Error(), "destination merges failed 3 non-memory times") || !strings.Contains(err.Error(), "merge failed") {
		t.Fatalf("error = %v, want repeated merge failure with ClickHouse exception", err)
	}
}

func TestObserveCompactProgressHalvesMergeBlockBytesAfterMemoryFailure(t *testing.T) {
	var recoveryQueries []string
	optimized := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		query := string(body)
		switch {
		case query == "SYSTEM FLUSH LOGS":
		case strings.Contains(query, "FROM system.part_log"):
			_, _ = io.WriteString(w, "1\t1\tCode: 241. DB::Exception: MEMORY_LIMIT_EXCEEDED\n")
		case query == "SYSTEM STOP MERGES `db`.`events`":
			recoveryQueries = append(recoveryQueries, query)
		case strings.HasPrefix(query, "ALTER TABLE"):
			recoveryQueries = append(recoveryQueries, query)
		case query == "SYSTEM START MERGES `db`.`events`":
			recoveryQueries = append(recoveryQueries, query)
		case strings.HasPrefix(query, "OPTIMIZE TABLE"):
			optimized <- query
		case strings.HasPrefix(query, "SELECT partition_id, count()"):
			_, _ = io.WriteString(w, "202607\t2\t100\t1000\n")
		case strings.Contains(query, "FROM system.merges"):
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	err := (Compactor{
		ProgressInterval: time.Nanosecond,
		ReportProgress: func(context.Context, CompactWorkItem, CompactProgressSnapshot) error {
			select {
			case query := <-optimized:
				if query != "OPTIMIZE TABLE `db`.`events` PARTITION ID '202607' FINAL SETTINGS optimize_throw_if_noop = 1" {
					t.Fatalf("optimize query = %q", query)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for optimize retry")
			}
			cancel()
			return nil
		},
	}).observeCompactProgress(ctx, Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
		MergeTreeSettings: MergeTreeSettings{
			MergeMaxBlockSizeBytes: 8 * 1024 * 1024,
		},
	}, CompactWorkItem{JobID: "job-1", OutputPartID: "compact-1"}, mergeWaitTarget{Database: "db", Table: "events"}, metrics.PartStats{Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	wantRecovery := []string{
		"SYSTEM STOP MERGES `db`.`events`",
		"ALTER TABLE `db`.`events` MODIFY SETTING merge_max_block_size_bytes = 4194304",
		"SYSTEM START MERGES `db`.`events`",
	}
	if !slices.Equal(recoveryQueries, wantRecovery) {
		t.Fatalf("queries = %#v, want recovery sequence %#v", recoveryQueries, wantRecovery)
	}
}

func TestCompactMergesIncludesSourceRowTotals(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		query := string(body)
		switch {
		case strings.Contains(query, "FROM system.merges"):
			if got := r.URL.Query().Get("output_format_json_quote_64bit_integers"); got != "0" {
				t.Fatalf("JSON integer quote setting = %q, want 0", got)
			}
			_, _ = io.WriteString(w, `{"partition_id":"202607","result_part_name":"all_1_2_1","elapsed":2.5,"progress":1.2,"num_parts":2,"source_part_names":["all_1_1_0","all_2_2_0"],"rows_read":40,"bytes_read_uncompressed":400,"total_size_bytes_uncompressed":1000}`+"\n")
		case strings.Contains(query, "FROM system.parts"):
			_, _ = io.WriteString(w, "all_1_1_0\t60\nall_2_2_0\t40\n")
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()

	merges, err := (Processor{ClickHouse: chhttp.Client{URL: server.URL}}).compactMerges(context.Background(), mergeWaitTarget{
		Database: "db",
		Table:    "table",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(merges) != 1 {
		t.Fatalf("merges = %+v, want one", merges)
	}
	merge := merges[0]
	if merge.PartitionID != "202607" || merge.ResultPartName != "all_1_2_1" || merge.Progress != 1.2 || merge.SourceParts != 2 || merge.RowsRead != 40 || merge.RowsTotal != 100 || merge.BytesRead != 400 || merge.BytesTotal != 1000 {
		t.Fatalf("merge = %+v", merge)
	}
}

func TestCompactMergeSummaryWeightsByBytes(t *testing.T) {
	active, progress := compactMergeSummary([]metrics.CompactMerge{
		{Progress: 0.25, BytesTotal: 100},
		{Progress: 0.75, BytesTotal: 300},
	})
	if active != 2 || progress != 0.625 {
		t.Fatalf("summary = active %d progress %f, want 2 and 0.625", active, progress)
	}
}
