package rewrite

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/partforge/partforge/internal/chhttp"
)

func TestConfigureCompactMergeSettingsDisablesVerticalMerges(t *testing.T) {
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
			MergeMaxBlockSize:      32768,
			MergeMaxBlockSizeBytes: 67108864,
			MergeSelectingSleepMS:  1000,
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
	if !strings.Contains(queries[0], "enable_vertical_merge_algorithm = 0") {
		t.Fatalf("query = %q, want vertical merge disabled", queries[0])
	}
}
