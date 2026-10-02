package chhttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestEndpointIncludesQueryIDAndSettings(t *testing.T) {
	client := Client{URL: "http://clickhouse:8123/?database=default"}
	endpoint, err := client.endpoint(QueryOptions{
		QueryID: "partforge-query",
		Settings: QuerySettings{
			"max_threads":        "8",
			"max_insert_threads": "8",
			"max_memory_usage":   "12345",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	if query.Get("database") != "default" {
		t.Fatalf("database = %q", query.Get("database"))
	}
	if query.Get("query_id") != "partforge-query" {
		t.Fatalf("query_id = %q", query.Get("query_id"))
	}
	if query.Get("max_threads") != "8" {
		t.Fatalf("max_threads = %q", query.Get("max_threads"))
	}
	if query.Get("max_insert_threads") != "8" {
		t.Fatalf("max_insert_threads = %q", query.Get("max_insert_threads"))
	}
	if query.Get("max_memory_usage") != "12345" {
		t.Fatalf("max_memory_usage = %q", query.Get("max_memory_usage"))
	}
}

func TestEndpointRejectsEmptySettingName(t *testing.T) {
	client := Client{URL: "http://clickhouse:8123/"}
	if _, err := client.endpoint(QueryOptions{Settings: QuerySettings{"": "1"}}); err == nil {
		t.Fatal("expected empty setting name error")
	}
}

func TestExecWithSummary(t *testing.T) {
	for _, header := range []string{
		`{"read_rows":"6","read_bytes":"60","total_rows_to_read":"6","written_rows":"3","written_bytes":"30"}`,
		"", "invalid",
	} {
		t.Run(header, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("wait_end_of_query") != "1" {
					t.Error("query response must be buffered until completion")
				}
				w.Header().Set("X-ClickHouse-Summary", header)
			}))
			defer server.Close()
			settings := QuerySettings{"max_threads": "2"}
			summary, err := (Client{URL: server.URL}).ExecWithSummary(context.Background(), "INSERT INTO dst SELECT * FROM src", QueryOptions{Settings: settings})
			if header == "" || header == "invalid" {
				if err == nil {
					t.Fatal("expected malformed or missing summary to fail")
				}
				return
			}
			if err != nil || summary != (QuerySummary{6, 60, 6, 3, 30}) {
				t.Fatalf("summary=%+v err=%v", summary, err)
			}
			if len(settings) != 1 {
				t.Fatal("mutated caller settings")
			}
		})
	}
}
