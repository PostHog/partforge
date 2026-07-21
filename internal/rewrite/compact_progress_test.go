package rewrite

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/metrics"
)

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
