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
					query == "SYSTEM FLUSH LOGS", strings.Contains(query, "system.query_log"):
				default:
					t.Errorf("unexpected query: %s", query)
				}
			}))
			defer server.Close()
			err := (Processor{
				ClickHouse: chhttp.Client{URL: server.URL}, InsertChunkMinRows: 2,
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
