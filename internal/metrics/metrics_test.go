package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PostHog/partforge/internal/manifest"
)

func TestQueryProgressMetricsIncludeAndClearTotalRowsApprox(t *testing.T) {
	prom := NewPrometheus()
	m := manifest.Manifest{JobID: "job-1", PartID: "part-1"}
	prom.ObserveProgress(m, QueryProgress{}, QueryProgress{ReadRows: 25, TotalRowsApprox: 100})

	body := compactMetricsBody(t, prom)
	for _, want := range []string{
		`partforge_current_read_rows{job_id="job-1",part_id="part-1"} 25`,
		`partforge_current_total_rows_approx{job_id="job-1",part_id="part-1"} 100`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}

	prom.ClearCurrentProgress(m)
	body = compactMetricsBody(t, prom)
	if !strings.Contains(body, `partforge_current_total_rows_approx{job_id="job-1",part_id="part-1"} 0`) {
		t.Fatalf("metrics body did not clear total rows estimate:\n%s", body)
	}
}

func TestInsertRetryMetrics(t *testing.T) {
	prom := NewPrometheus()
	m := manifest.Manifest{JobID: "job-1", PartID: "part-1"}
	prom.InsertSelectStarted(m)
	if body := compactMetricsBody(t, prom); !strings.Contains(body, `partforge_insert_attempt_duration_seconds_count{destination_table=".",job_id="job-1",result="failed",source_table="."} 0`) {
		t.Fatalf("missing initial zero-valued attempt counter:\n%s", body)
	}
	prom.ObserveInsertAttempt(m, "failed", 8*time.Second)
	prom.ObserveInsertAttempt(m, "failed", 9*time.Second)
	prom.ObserveInsertAttempt(m, "completed", 10*time.Second)
	// Two failed attempts plus three seconds of recovery: 30s total / 10s useful = 3x.
	prom.ObserveInsertSelect(m, "completed", 3, 30*time.Second, 20*time.Second)
	prom.InsertSelectStarted(m) // Starting the next part must not reset counters.
	prom.ObserveInsertSelect(m, "completed", 1, 5*time.Second, 0)
	prom.ObserveInsertSelect(m, "failed", 1, 2*time.Second, 2*time.Second)
	body := compactMetricsBody(t, prom)
	for _, want := range []string{
		`partforge_insert_attempt_duration_seconds_count{destination_table=".",job_id="job-1",result="failed",source_table="."} 2`,
		`partforge_insert_attempt_duration_seconds_sum{destination_table=".",job_id="job-1",result="failed",source_table="."} 17`,
		`partforge_insert_attempt_duration_seconds_count{destination_table=".",job_id="job-1",result="completed",source_table="."} 1`,
		`partforge_insert_duration_seconds_sum{destination_table=".",job_id="job-1",result="completed",retried="true",source_table="."} 30`,
		`partforge_insert_duration_seconds_count{destination_table=".",job_id="job-1",result="completed",retried="true",source_table="."} 1`,
		`partforge_insert_wasted_seconds_total{destination_table=".",job_id="job-1",result="completed",retried="true",source_table="."} 20`,
		`partforge_insert_wasted_seconds_total{destination_table=".",job_id="job-1",result="completed",retried="false",source_table="."} 0`,
		`partforge_insert_wasted_seconds_total{destination_table=".",job_id="job-1",result="failed",retried="false",source_table="."} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestCompactProgressMetricsReconcileAndClear(t *testing.T) {
	prom := NewPrometheus()
	prom.CompactionStarted("job-1", "compact-1", 2, PartStats{Count: 8, Rows: 800, Bytes: 8_000})
	prom.SetCompactPartitionStats("input", "job-1", "compact-1", []CompactPartitionStats{
		{PartitionID: "202607", Stats: PartStats{Count: 8, Rows: 800, Bytes: 8_000}},
	})
	prom.ObserveCompactProgress("job-1", "compact-1", "merging", PartStats{Count: 8, Rows: 800, Bytes: 8_000}, []CompactPartitionStats{
		{PartitionID: "202607", Stats: PartStats{Count: 8, Rows: 800, Bytes: 8_000}},
	}, []CompactMerge{
		{PartitionID: "202607", ResultPartName: "all_1_8_1", Elapsed: 3 * time.Second, Progress: 0.25, SourceParts: 8, RowsRead: 200, RowsTotal: 800, BytesRead: 2_000, BytesTotal: 8_000},
	})

	body := compactMetricsBody(t, prom)
	for _, want := range []string{
		`partforge_compact_batch_active{job_id="job-1",output_part_id="compact-1"} 1`,
		`partforge_compact_stage{job_id="job-1",output_part_id="compact-1",stage="merging"} 1`,
		`partforge_compact_merge_progress_ratio{job_id="job-1",output_part_id="compact-1",partition_id="202607",result_part_name="all_1_8_1"} 0.25`,
		`partforge_compact_merge_rows_total{job_id="job-1",output_part_id="compact-1",partition_id="202607",result_part_name="all_1_8_1"} 800`,
		`partforge_compact_partition_parts{job_id="job-1",output_part_id="compact-1",partition_id="202607",role="current"} 8`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}

	prom.ObserveCompactProgress("job-1", "compact-1", "waiting_for_merge_selection", PartStats{Count: 2}, nil, nil)
	body = compactMetricsBody(t, prom)
	if strings.Contains(body, `stage="merging"`) || strings.Contains(body, `result_part_name="all_1_8_1"`) || strings.Contains(body, `role="current"`) {
		t.Fatalf("metrics body retained stale compact labels:\n%s", body)
	}
	if !strings.Contains(body, `stage="waiting_for_merge_selection"`) {
		t.Fatalf("metrics body missing replacement compact stage:\n%s", body)
	}

	prom.ClearCompaction("job-1", "compact-1")
	body = compactMetricsBody(t, prom)
	for _, stale := range []string{`output_part_id="compact-1"`, `partforge_compact_input_artifacts{job_id="job-1"`} {
		if strings.Contains(body, stale) {
			t.Fatalf("metrics body retained %q after cleanup:\n%s", stale, body)
		}
	}
}

func compactMetricsBody(t *testing.T, prom *Prometheus) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	prom.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func TestHandlerServesPartForgeMetricsWhenClickHouseInactive(t *testing.T) {
	prom := NewPrometheus()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	prom.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "partforge_clickhouse_metrics_up 0") {
		t.Fatalf("body missing inactive ClickHouse up metric:\n%s", body)
	}
}

func TestHandlerMergesClickHouseMetrics(t *testing.T) {
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			t.Fatalf("path = %q, want /metrics", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte("# HELP ClickHouseMetrics_Test A ClickHouse test metric.\n# TYPE ClickHouseMetrics_Test gauge\nClickHouseMetrics_Test 3\n"))
	}))
	defer clickHouse.Close()

	prom := NewPrometheus()
	prom.SetClickHouseTarget(clickHouse.URL + "/metrics")
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	prom.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "ClickHouseMetrics_Test 3") {
		t.Fatalf("body missing ClickHouse metric:\n%s", body)
	}
	if !strings.Contains(body, "partforge_clickhouse_metrics_up 1") {
		t.Fatalf("body missing active ClickHouse up metric:\n%s", body)
	}
}

func TestHandlerFailsWhenActiveClickHouseScrapeFails(t *testing.T) {
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer clickHouse.Close()

	prom := NewPrometheus()
	prom.SetClickHouseTarget(clickHouse.URL + "/metrics")
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	prom.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadGateway, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "status 503") {
		t.Fatalf("body missing ClickHouse scrape status: %s", rec.Body.String())
	}
}

func TestHandlerFailsOnDuplicateClickHouseMetricFamily(t *testing.T) {
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte("# HELP partforge_clickhouse_metrics_up duplicate\n# TYPE partforge_clickhouse_metrics_up gauge\npartforge_clickhouse_metrics_up 1\n"))
	}))
	defer clickHouse.Close()

	prom := NewPrometheus()
	prom.SetClickHouseTarget(clickHouse.URL + "/metrics")
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	prom.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "duplicate metric family") {
		t.Fatalf("body missing duplicate metric error: %s", rec.Body.String())
	}
}
