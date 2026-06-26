package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
