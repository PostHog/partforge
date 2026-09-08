package rewrite

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/manifest"
	"github.com/PostHog/partforge/internal/metrics"
)

func TestInsertChunkCount(t *testing.T) {
	for _, tc := range []struct{ rows, minimum, want uint64 }{
		{0, DefaultInsertChunkMinRows, 1},
		{19_999_999, DefaultInsertChunkMinRows, 1},
		{20_000_000, DefaultInsertChunkMinRows, 2},
		{199_999_999, DefaultInsertChunkMinRows, 19},
		{200_000_000, DefaultInsertChunkMinRows, 20},
		{1_000_000_000_000, DefaultInsertChunkMinRows, 20},
		{^uint64(0), 1, 20},
		{^uint64(0), 0, 1},
	} {
		if got := insertChunkCount(tc.rows, tc.minimum); got != tc.want {
			t.Errorf("insertChunkCount(%d, %d) = %d, want %d", tc.rows, tc.minimum, got, tc.want)
		}
	}
}

func TestInsertChunksPreserveCompletedOutput(t *testing.T) {
	for _, failMove := range []bool{false, true} {
		t.Run(fmt.Sprintf("failMove=%t", failMove), func(t *testing.T) {
			m := manifest.Manifest{
				JobID: "job", PartID: "part",
				Source: manifest.TableRef{Database: "db", Table: "src"},
				Dest:   manifest.TableRef{Database: "db", Table: "dst"},
				SQL:    manifest.SQLBundle{InsertSelect: "INSERT INTO db.dst SELECT * FROM db.src"},
			}
			var snapshots []ProgressSnapshot
			var ranges, threads, ids []string
			var staged, completed []int
			inserts, truncates, moves := 0, 0, 0
			exchanged := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				query := string(body)
				switch {
				case query == m.SQL.InsertSelect:
					inserts++
					ranges = append(ranges, r.URL.Query().Get("additional_table_filters"))
					threads = append(threads, r.URL.Query().Get("max_threads"))
					ids = append(ids, r.URL.Query().Get("query_id"))
					switch inserts {
					case 1:
						staged = []int{0, 1, 2}
					case 2:
						// A failed INSERT can already have written some parts.
						staged = []int{3}
						http.Error(w, "Code: 241. MEMORY_LIMIT_EXCEEDED", 500)
					case 3:
						staged = append(staged, 3, 4)
					case 4:
						// Last range is filtered out: it must still complete.
					default:
						t.Errorf("unexpected insert attempt %d", inserts)
					}
				case query == "TRUNCATE TABLE `db`.`dst` SYNC":
					truncates++
					staged = nil
					if !reflect.DeepEqual(completed, []int{0, 1, 2}) {
						t.Errorf("retry lost completed rows: %v", completed)
					}
				case strings.HasPrefix(query, "SELECT partition_id,"):
					if len(staged) > 0 {
						fmt.Fprintf(w, "p0\t1\t%d\t10\np1\t1\t0\t10\n", len(staged))
					}
				case strings.HasPrefix(query, "SELECT count(),") && strings.Contains(query, "system.parts"):
					fmt.Fprint(w, "1\t7\t100\n")
				case query == "ALTER TABLE `db`.`dst` MOVE PARTITION ID 'p0' TO TABLE `db`.`dst__partforge_completed`":
					moves++
					completed = append(completed, staged...)
					staged = nil
				case query == "ALTER TABLE `db`.`dst` MOVE PARTITION ID 'p1' TO TABLE `db`.`dst__partforge_completed`":
					if failMove {
						// Even a resource error during promotion must not retry INSERT.
						http.Error(w, "Code: 241. MEMORY_LIMIT_EXCEEDED", 500)
					}
				case query == "EXCHANGE TABLES `db`.`dst` AND `db`.`dst__partforge_completed`":
					exchanged = true
				case query == "CREATE TABLE `db`.`dst__partforge_completed` AS `db`.`dst`",
					query == "DROP TABLE `db`.`dst__partforge_completed` SYNC",
					query == "SYSTEM FLUSH LOGS":
				case strings.Contains(query, "system.query_log"):
					switch inserts {
					case 1:
						fmt.Fprint(w, "6\t60\t6\t3\t30\n")
					case 3:
						fmt.Fprint(w, "4\t40\t4\t2\t20\n")
					case 4:
						fmt.Fprint(w, "0\t0\t0\t0\t0\n")
					}
				default:
					t.Errorf("unexpected query: %s", query)
				}
			}))
			defer server.Close()
			err := (Processor{
				ClickHouse: chhttp.Client{URL: server.URL}, InsertChunkMinRows: 2,
				ReportProgress: func(_ context.Context, _ manifest.Manifest, snapshot ProgressSnapshot) error {
					snapshots = append(snapshots, snapshot)
					return nil
				},
				InsertSettings: chhttp.QuerySettings{"max_threads": "4", "max_insert_threads": "4", "max_block_size": "8192"},
			}).runInsertSelectWithRetries(context.Background(), m, 7)
			if failMove {
				if err == nil || !strings.Contains(err.Error(), "promote insert partition p1") || inserts != 1 || truncates != 0 || exchanged {
					t.Fatalf("promotion failure retried or completed: err=%v inserts=%d truncates=%d exchanged=%t", err, inserts, truncates, exchanged)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			last := snapshots[len(snapshots)-1]
			if last.InsertProgressPercent == nil || *last.InsertProgressPercent != 100 ||
				last.QueryProgress.ReadRows != 10 || last.QueryProgress.ReadBytes != 100 ||
				last.QueryProgress.WrittenRows != 5 || last.QueryProgress.WrittenBytes != 50 {
				t.Fatalf("lost cumulative progress or empty final chunk: %+v / %+v", last, last.QueryProgress)
			}
			for _, snapshot := range snapshots {
				if snapshot.InsertProgressPercent == nil {
					t.Fatal("chunked attempt omitted whole-part progress")
				}
			}
			wantRanges := []string{insertChunkFilter(m.Source, 0, 3), insertChunkFilter(m.Source, 3, 5), insertChunkFilter(m.Source, 3, 5), insertChunkFilter(m.Source, 5, 7)}
			if !reflect.DeepEqual(ranges, wantRanges) || !reflect.DeepEqual(threads, []string{"4", "4", "2", "2"}) {
				t.Fatalf("unexpected retries: ranges=%v threads=%v", ranges, threads)
			}
			if !reflect.DeepEqual(completed, []int{0, 1, 2, 3, 4}) || truncates != 1 || moves != 2 || !exchanged {
				t.Fatalf("unexpected output: rows=%v truncates=%d moves=%d exchanged=%t", completed, truncates, moves, exchanged)
			}
			for i, id := range ids {
				if id != fmt.Sprintf("partforge-job-part-attempt-%d", i+1) {
					t.Fatalf("query IDs must remain unique across chunks: %v", ids)
				}
			}
		})
	}
}

func TestInsertChunksRejectFilterOverride(t *testing.T) {
	err := (Processor{InsertChunkMinRows: 1}).runInsertSelectWithRetries(context.Background(), manifest.Manifest{
		SQL: manifest.SQLBundle{InsertSelect: "INSERT INTO dst SELECT * FROM src SETTINGS additional_table_filters = {}"},
	}, 2)
	if err == nil || !strings.Contains(err.Error(), "reserves additional_table_filters") {
		t.Fatalf("expected rejected filter override, got %v", err)
	}
}

func TestInsertProgress(t *testing.T) {
	p := insertProgress{start: 20, end: 40, total: 100,
		completed: metrics.QueryProgress{ReadRows: 60, ReadBytes: 600, WrittenRows: 10, WrittenBytes: 100, TotalRowsApprox: 300}}
	for _, tc := range []struct {
		name         string
		current      metrics.QueryProgress
		committed    bool
		percent      float64
		reads, total uint64
	}{
		{"new attempt retains completed work", metrics.QueryProgress{}, false, 20, 60, 300},
		{"half of second chunk", metrics.QueryProgress{ReadRows: 30, TotalRowsApprox: 60}, false, 30, 90, 300},
		{"clamp read ahead to current chunk", metrics.QueryProgress{ReadRows: 70, TotalRowsApprox: 60}, false, 40, 130, 300},
		{"empty chunk committed", metrics.QueryProgress{}, true, 40, 60, 300},
		{"unknown estimate stays at completed boundary", metrics.QueryProgress{ReadRows: 30}, false, 20, 90, 300},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := p.snapshot(tc.current, tc.committed)
			if got.InsertProgressPercent == nil || *got.InsertProgressPercent != tc.percent ||
				got.QueryProgress.ReadRows != tc.reads || got.QueryProgress.TotalRowsApprox != tc.total {
				t.Fatalf("progress = %+v / %+v, want percent=%g reads=%d total=%d", got, got.QueryProgress, tc.percent, tc.reads, tc.total)
			}
		})
	}
	// Empty completed input must still show 100%, even without read counters.
	empty := (insertProgress{start: 80, end: 100, total: 100}).snapshot(metrics.QueryProgress{}, true)
	if *empty.InsertProgressPercent != 100 {
		t.Fatalf("empty final chunk = %+v", empty)
	}
	current := metrics.QueryProgress{ReadRows: 5, TotalRowsApprox: 20}
	unchunked := (insertProgress{}).snapshot(current, false)
	if unchunked.InsertProgressPercent != nil || *unchunked.QueryProgress != current {
		t.Fatalf("changed unchunked progress: %+v", unchunked)
	}
}
