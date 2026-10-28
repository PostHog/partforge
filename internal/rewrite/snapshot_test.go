package rewrite

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PostHog/partforge/internal/chhttp"
)

func TestSnapshotWaitsForInFlightMerge(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	counts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		q := string(b)
		mu.Lock()
		defer mu.Unlock()
		queries = append(queries, q)
		if strings.HasPrefix(q, "SELECT count() FROM system.merges") {
			counts++
			if counts == 1 {
				_, _ = io.WriteString(w, "1\n")
			} else {
				_, _ = io.WriteString(w, "0\n")
			}
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	p := Processor{ClickHouse: chhttp.Client{URL: server.URL}, MergeTimeout: time.Nanosecond, MergePollInterval: time.Millisecond}
	resume, err := p.pauseDestinationMerges(ctx, testMergeWaitTarget())
	if err != nil {
		t.Fatal(err)
	}
	// Cancellation after the snapshot must not prevent restoring merges.
	cancel()
	if err := resume(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(queries) != 4 || !strings.HasPrefix(queries[0], "SYSTEM STOP MERGES ") ||
		counts != 2 || !strings.HasPrefix(queries[3], "SYSTEM START MERGES ") {
		t.Fatalf("snapshot queries = %v", queries)
	}
}

func TestSnapshotDrainFailureRestoresMerges(t *testing.T) {
	for _, failure := range []string{"query", "timeout", "restore"} {
		t.Run(failure, func(t *testing.T) {
			var mu sync.Mutex
			restored := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				q := string(b)
				switch {
				case strings.HasPrefix(q, "SELECT count() FROM system.merges"):
					if failure == "query" {
						http.Error(w, "cannot read merges", http.StatusInternalServerError)
					} else {
						_, _ = io.WriteString(w, "1\n")
					}
				case strings.HasPrefix(q, "SYSTEM START MERGES "):
					mu.Lock()
					restored = true
					mu.Unlock()
					if failure == "restore" {
						http.Error(w, "cannot restore merges", http.StatusInternalServerError)
					}
				}
			}))
			defer server.Close()
			p := Processor{ClickHouse: chhttp.Client{URL: server.URL}, MergeTimeout: time.Nanosecond, MergePollInterval: time.Millisecond}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			resume, err := p.pauseDestinationMerges(ctx, testMergeWaitTarget())
			if err == nil || resume != nil || !strings.Contains(err.Error(), "drain destination merges") {
				t.Fatalf("pause = %v, resume nil = %t", err, resume == nil)
			}
			if failure == "restore" && !strings.Contains(err.Error(), "restore destination merges") {
				t.Fatalf("lost restore error: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !restored {
				t.Fatal("did not restore merges after failed snapshot guard")
			}
		})
	}
}
