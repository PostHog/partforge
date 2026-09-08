package state

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func postgresTestConfig(t testing.TB) Config {
	t.Helper()
	url := os.Getenv("PARTFORGE_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set PARTFORGE_TEST_POSTGRES_URL to run PostgreSQL integration checks")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("partforge_test_%d", time.Now().UnixNano())
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		conn.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := conn.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error(err)
		}
		conn.Close(ctx)
	})
	return Config{Endpoint: url, Table: schema + ".parts"}
}

func postgresTestStore(t testing.TB) *Store {
	t.Helper()
	cfg := postgresTestConfig(t)
	if _, err := Migrate(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.pool.Close)
	t.Cleanup(func() { assertSchedulingColumns(t, s) })
	return s
}

func seedPostgresParts(t testing.TB, s *Store, parts []Part) {
	t.Helper()
	rows := make([][]any, 0, len(parts))
	for _, part := range parts {
		values, err := partWriteValues(part)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, values)
	}
	if _, err := s.pool.CopyFrom(context.Background(), pgx.Identifier(strings.Split(s.tableName, ".")), strings.Split(partColumns, ", "), pgx.CopyFromRows(rows)); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresMigrations(t *testing.T) {
	ctx := context.Background()
	cfg := postgresTestConfig(t)
	if _, err := New(ctx, cfg); err == nil || !strings.Contains(err.Error(), "migrate") {
		t.Fatalf("unmigrated schema error = %v", err)
	}
	const runners = 4
	var wg sync.WaitGroup
	applied := make(chan int, runners)
	errs := make(chan error, runners)
	for i := 0; i < runners; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); n, err := Migrate(ctx, cfg); applied <- n; errs <- err }()
	}
	wg.Wait()
	close(applied)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	total := 0
	for n := range applied {
		total += n
	}
	s, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.pool.Close()
	if total != len(s.migrations()) {
		t.Fatalf("applied %d migrations", total)
	}
	if n, err := Migrate(ctx, cfg); err != nil || n != 0 {
		t.Fatalf("repeat migration = %d, %v", n, err)
	}
	// Qualified and search-path-qualified names address the same migration ledger.
	alias := cfg
	name := strings.Split(cfg.Table, ".")
	alias.Table = name[1]
	alias.Endpoint += "&search_path=" + name[0]
	if n, err := Migrate(ctx, alias); err != nil || n != 0 {
		t.Fatalf("aliased migration = %d, %v", n, err)
	}
	aliasedStore, err := New(ctx, alias)
	if err != nil {
		t.Fatal(err)
	}
	aliasedStore.pool.Close()

	if _, err := s.pool.Exec(ctx, `INSERT INTO `+s.relatedSQL("migrations")+` (version) VALUES (99)`); err != nil {
		t.Fatal(err)
	}
	if _, err := New(ctx, cfg); err == nil {
		t.Fatal("accepted unsupported schema version")
	}
	if _, err := Migrate(ctx, cfg); err == nil {
		t.Fatal("migrated unsupported history")
	}
}

func TestPostgresLegacyMigrationIsAtomic(t *testing.T) {
	ctx := context.Background()
	cfg := postgresTestConfig(t)
	s, err := openStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.pool.Close()
	if _, err := s.pool.Exec(ctx, s.migrations()[0]); err != nil {
		t.Fatal(err)
	}
	part := NewPart("legacy", "part", "bucket", "source", "finished", time.Now())
	part.SourceArtifactBytes = 12345
	legacyData, err := partJSON(part)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO `+s.tableSQL+` (job_id,part_id,status,worker_id,created_at,updated_at,data) VALUES ($1,$2,$3,$4,$5,$6,$7)`, part.JobID, part.PartID, string(part.Status), part.WorkerID, part.CreatedAt, part.UpdatedAt, legacyData); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE `+s.tableSQL+` SET data = data || '{"source_artifact_bytes":"invalid"}'::jsonb`); err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(ctx, cfg); err == nil {
		t.Fatal("migration accepted invalid size")
	}
	var added bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT FROM pg_attribute WHERE attrelid=$1::regclass AND attname='source_artifact_bytes')`, s.tableSQL).Scan(&added); err != nil {
		t.Fatal(err)
	}
	if added {
		t.Fatal("failed migration left schema changes")
	}
	data, err := partJSON(part)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE `+s.tableSQL+` SET data=$1::jsonb || '{"compact_input_part_ids":null,"destination_active_partition_counts":null}'::jsonb`, data); err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	assertSchedulingColumns(t, s)
	var size string
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT source_artifact_bytes::text, count(*) OVER() FROM `+s.tableSQL).Scan(&size, &count); err != nil {
		t.Fatal(err)
	}
	if size != "12345" || count != 1 {
		t.Fatalf("legacy data changed: size=%s count=%d", size, count)
	}
	if _, err := s.ListJobs(ctx); err != nil {
		t.Fatalf("legacy optional null fields: %v", err)
	}

}

func TestPostgresClaimsAndProgress(t *testing.T) {
	ctx := context.Background()
	s := postgresTestStore(t)
	now := time.Now().UTC()
	parts := make([]Part, 64)
	for i := range parts {
		parts[i] = NewPart(fmt.Sprintf("job-%d", i%10), fmt.Sprintf("part-%03d", i), "bucket", "source", "finished", now)
		parts[i].SourceArtifactBytes = uint64(i + 1)
	}
	seedPostgresParts(t, s, parts)
	// A locked highest-priority part must not stall another claimant.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT FROM `+s.tableSQL+` WHERE part_id='part-063' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	timed, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	first, err := s.ClaimNextReady(timed, "first", now)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || first.PartID != "part-062" {
		t.Fatalf("first claim = %+v", first)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	claimed := make(chan *Part, 63)
	errs := make(chan error, 63)
	for i := 0; i < 63; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p, err := s.ClaimNextReady(ctx, fmt.Sprint(i), now)
			claimed <- p
			errs <- err
		}(i)
	}
	wg.Wait()
	close(claimed)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{first.PartID: true}
	for p := range claimed {
		if p == nil || seen[p.PartID] {
			t.Fatalf("duplicate or missing claim: %+v", p)
		}
		seen[p.PartID] = true
	}
	if len(seen) != 64 {
		t.Fatalf("claimed %d parts", len(seen))
	}
	if p, err := s.ClaimNextReady(ctx, "empty", now); err != nil || p != nil {
		t.Fatalf("empty queue: %+v %v", p, err)
	}
	if err := s.UpdateRewriteProgress(ctx, first.JobID, first.PartID, "wrong", RewriteProgress{}, now); !IsConditionalCheckFailed(err) {
		t.Fatalf("ownership error = %v", err)
	}
	if err := s.UpdateRewriteProgress(ctx, first.JobID, first.PartID, "first", RewriteProgress{QueryProgress: &QueryProgress{ReadRows: 42}, SourceActivePartStats: &PartStats{Count: 3}}, now); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateRewriteProgress(ctx, first.JobID, first.PartID, "first", RewriteProgress{QueryProgress: &QueryProgress{}}, now); err != nil {
		t.Fatal(err)
	}
	current, err := s.ListJobParts(ctx, first.JobID)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range current {
		if p.PartID == first.PartID && (p.ReadRows != 0 || p.SourceActivePartCount != 3 || p.SourceArtifactBytes != 63) {
			t.Fatalf("progress patch lost fields: %+v", p)
		}
	}
}

func TestPostgresCompactScheduling(t *testing.T) {
	ctx := context.Background()
	s := postgresTestStore(t)
	now := time.Now().UTC()
	parts := make([]Part, 20)
	for i := range parts {
		p := NewPart("job", fmt.Sprintf("part-%02d", i), "bucket", "source", "finished", now)
		p.Status = StatusCompactReady
		p.CompactReadyAt = formatTime(now)
		p.DestinationDatabase = "db"
		p.DestinationTable = "table"
		p.DestinationSchema = "schema"
		p.DestinationActivePartCount = 2
		p.DestinationActivePartBytes = uint64(i + 1)
		p.DestinationActivePartitionCounts = map[string]uint64{"p": 2}
		parts[i] = p
	}
	seedPostgresParts(t, s, parts)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT FROM `+s.tableSQL+` WHERE part_id='part-19' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	timed, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	batch, err := s.ClaimNextCompactBatch(timed, "worker", now, CompactClaimOptions{CompactWindow: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil || batch.Parts[0].PartID != "part-18" {
		t.Fatalf("compact claim: %+v", batch)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// All contenders originally picked a single part in this one destination group.
	var wg sync.WaitGroup
	claimed := make(chan *CompactBatch, 19)
	errs := make(chan error, 19)
	for i := 0; i < 19; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b, err := s.ClaimNextCompactBatch(ctx, fmt.Sprint(i), now, CompactClaimOptions{})
			claimed <- b
			errs <- err
		}(i)
	}
	wg.Wait()
	close(claimed)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{batch.Parts[0].PartID: true}
	for b := range claimed {
		if b == nil || seen[b.Parts[0].PartID] {
			t.Fatalf("duplicate or missing compact claim: %+v", b)
		}
		seen[b.Parts[0].PartID] = true
	}
	if n, err := s.FinalizeCompactReadyJob(ctx, "job", time.Hour, now); err != nil || n != 0 {
		t.Fatalf("active job finalized: %d %v", n, err)
	}
	if n, err := s.ReleaseStaleCompactingParts(ctx, now.Add(2*time.Hour), time.Hour); err != nil || n != 20 {
		t.Fatalf("stale release: %d %v", n, err)
	}
	if b, err := s.ClaimNextCompactBatch(ctx, "late", now.Add(2*time.Hour), CompactClaimOptions{CompactWindow: time.Hour}); err != nil || b != nil {
		t.Fatalf("claimed expired work: %+v %v", b, err)
	}
	if n, err := s.MaintainCompaction(ctx, time.Hour, time.Hour, now.Add(2*time.Hour), false); err != nil || n != 20 {
		t.Fatalf("finalized: %d %v", n, err)
	}
	// The shared cadence must skip a second scan even when new normalized work arrives.
	normalized := parts[0]
	normalized.JobID = "new"
	normalized.PartID = "normalized"
	normalized.DestinationActivePartCount = 1
	normalized.DestinationActivePartitionCounts = map[string]uint64{"p": 1}
	seedPostgresParts(t, s, []Part{normalized})
	if n, err := s.MaintainCompaction(ctx, time.Hour, time.Hour, now.Add(2*time.Hour), false); err != nil || n != 0 {
		t.Fatalf("maintenance ran twice: %d %v", n, err)
	}
	if n, err := s.MaintainCompaction(ctx, time.Hour, time.Hour, now.Add(2*time.Hour), true); err != nil || n != 1 {
		t.Fatalf("normalized finalization: %d %v", n, err)
	}
}

func TestPostgresCompactOptionsAndSummaries(t *testing.T) {
	ctx := context.Background()
	s := postgresTestStore(t)
	now := time.Now().UTC()
	makePart := func(job, id, partition string, size uint64, status Status) Part {
		p := NewPart(job, id, "bucket", "source", "finished", now)
		p.Status = status
		p.JobName = "name"
		p.CompactReadyAt = formatTime(now)
		p.DestinationDatabase = "db"
		p.DestinationTable = "table"
		p.DestinationSchema = "schema"
		p.DestinationActivePartCount = 2
		p.DestinationActivePartBytes = size
		p.DestinationActivePartitionCounts = map[string]uint64{partition: 2}
		if status == StatusCompacting {
			p.WorkerID = "busy"
			p.CompactingAt = formatTime(now)
		}
		return p
	}
	parts := []Part{makePart("a", "busy", "busy", 1, StatusCompacting), makePart("a", "large", "busy", 1000, StatusCompactReady), makePart("a", "idle", "idle", 100, StatusCompactReady), makePart("b", "other", "busy", 200, StatusCompactReady)}
	seedPostgresParts(t, s, parts)
	// Partition filters remain explicit; busy partitions no longer change priority.
	for _, test := range []struct {
		opts CompactClaimOptions
		want string
	}{
		{CompactClaimOptions{}, "large"}, {CompactClaimOptions{JobID: "a"}, "large"},
		{CompactClaimOptions{RequiredPartitionIDs: []string{"busy"}}, "large"},
		{CompactClaimOptions{RequiredPartitionIDs: []string{"idle"}}, "idle"},
		{CompactClaimOptions{JobID: "b"}, "other"},
		{CompactClaimOptions{ExcludedJobIDs: map[string]struct{}{"a": {}}}, "other"},
		{CompactClaimOptions{Bucket: "other"}, ""}, {CompactClaimOptions{DestinationDatabase: "other"}, ""},
		{CompactClaimOptions{DestinationTable: "other"}, ""}, {CompactClaimOptions{DestinationSchema: "other"}, ""},
	} {
		opts := test.opts
		batch, err := s.ClaimNextCompactBatch(ctx, "test", now, opts)
		if err != nil {
			t.Fatal(err)
		}
		if test.want == "" {
			if batch != nil {
				t.Fatalf("unexpected claim for %+v: %+v", opts, batch)
			}
			continue
		}
		if batch == nil || batch.Parts[0].PartID != test.want {
			t.Fatalf("claim for %+v = %+v, want %s", opts, batch, test.want)
		}
		if err := s.RequestCompactFinalization(ctx, batch.Parts[0], now); err != nil {
			t.Fatal(err)
		}
		if err := s.UpdateCompactProgress(ctx, *batch, "output", "test", PartStats{Count: 2}, PartStats{Count: 1}, CompactProgress{Stage: "merging"}, now); err != nil {
			t.Fatal(err)
		}
		if _, err := s.HeartbeatCompactBatch(ctx, *batch, "wrong", now); !IsConditionalCheckFailed(err) {
			t.Fatalf("compact ownership error = %v", err)
		}
		if requested, err := s.HeartbeatCompactBatch(ctx, *batch, "test", now); err != nil || !requested {
			t.Fatalf("finalize request lost: %v %v", requested, err)
		}
		if err := s.ReleaseCompactBatch(ctx, *batch, "test", now); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := s.ListJobIDsByStatus(ctx, StatusCompactReady, StatusCompactReady)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ids, ",") != "a,b" {
		t.Fatalf("job IDs = %v", ids)
	}
	jobs, err := s.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].Total != 3 || jobs[0].Counts[StatusCompactReady] != 2 || jobs[0].Name != "name" || jobs[0].DestinationPartitionCount != 2 || jobs[0].DestinationActivePartCount != 6 {
		t.Fatalf("job summaries = %+v", jobs)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE `+s.tableSQL+` SET data=data || '{"job_name":"conflict"}'::jsonb WHERE part_id='large'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListJobs(ctx); err == nil {
		t.Fatal("accepted conflicting job names")
	}
}

func TestPostgresClaimPlans(t *testing.T) {
	ctx := context.Background()
	s := postgresTestStore(t)
	now := time.Now().UTC()
	parts := make([]Part, 20000)
	for i := range parts {
		p := NewPart(fmt.Sprintf("job-%d", i%10), fmt.Sprintf("part-%05d", i), "bucket", "source", "finished", now)
		p.SourceArtifactBytes = uint64(i + 1)
		p.DestinationActivePartBytes = uint64(i + 1)
		p.DestinationDatabase = "db"
		p.DestinationTable = "table"
		p.DestinationSchema = strings.Repeat("column String, ", 80)
		p.DestinationActivePartCount = 2
		p.DestinationActivePartitionCounts = map[string]uint64{"p": 2}
		p.CompactReadyAt = formatTime(now)
		if i%2 == 0 {
			p.Status = StatusCompactReady
		}
		parts[i] = p
	}
	seedPostgresParts(t, s, parts)
	if _, err := s.pool.Exec(ctx, "ANALYZE "+s.tableSQL); err != nil {
		t.Fatal(err)
	}
	compact, args := s.compactClaimQuery(CompactClaimOptions{CompactWindow: time.Hour}, now)
	expired, expiredArgs := s.compactClaimQuery(CompactClaimOptions{CompactWindow: time.Hour}, now.Add(2*time.Hour))
	for _, test := range []struct {
		name, query, index string
		args               []any
	}{
		{"rewrite", s.readyClaimQuery(), "ready_priority_idx", nil},
		{"compact", compact, "compact_priority_idx", args},
		{"expired compact", expired, "compact_priority_idx", expiredArgs},
	} {
		rows, err := s.pool.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS) "+test.query, test.args...)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		text := strings.Join(plan, "\n")
		t.Log(test.name + "\n" + text)
		if !strings.Contains(text, "Index Scan using "+strings.Trim(s.indexSQL(test.index), `"`)) || strings.Contains(text, "Sort Key:") {
			t.Fatalf("%s claim lost ordered index scan:\n%s", test.name, text)
		}
	}
}

// Check application writes against values derived independently from the JSON.
func assertSchedulingColumns(t testing.TB, s *Store) {
	t.Helper()
	var mismatches int
	err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM `+s.tableSQL+` WHERE
 source_artifact_bytes IS DISTINCT FROM COALESCE((data->>'source_artifact_bytes')::numeric, 0) OR
 compact_bytes IS DISTINCT FROM COALESCE((data->>'destination_active_part_bytes')::numeric, 0) OR
 compact_eligible IS DISTINCT FROM (
 COALESCE(btrim(data->>'destination_database'), '') <> '' AND
 COALESCE(btrim(data->>'destination_table'), '') <> '' AND
 COALESCE(btrim(data->>'destination_schema'), '') <> '' AND
 COALESCE((data->>'destination_active_part_count')::numeric, 0) > 0 AND
 EXISTS (SELECT FROM jsonb_each_text(COALESCE(NULLIF(data->'destination_active_partition_counts', 'null'::jsonb), '{}'::jsonb)) p WHERE btrim(p.key) <> '' AND p.value::numeric > 1)) OR
 compact_normalized IS DISTINCT FROM (
 COALESCE((data->>'destination_active_part_count')::numeric, 0) = 1 AND
 (SELECT count(*) = 1 AND COALESCE(bool_and(p.value::numeric = 1), false)
 FROM jsonb_each_text(COALESCE(NULLIF(data->'destination_active_partition_counts', 'null'::jsonb), '{}'::jsonb)) p WHERE btrim(p.key) <> '' AND p.value::numeric > 0)) OR
 compact_stale_at IS DISTINCT FROM (CASE WHEN status = 'COMPACTING' THEN LEAST(NULLIF(updated_at, '')::timestamptz, NULLIF(data->>'compacting_at', '')::timestamptz) END) OR
 original_compact_ready_at IS DISTINCT FROM (CASE WHEN COALESCE((data->>'compact_generation')::int, 0) <= 0 AND jsonb_array_length(COALESCE(NULLIF(data->'compact_input_part_ids', 'null'::jsonb), '[]'::jsonb)) = 0 THEN NULLIF(data->>'compact_ready_at', '')::timestamptz END)`).Scan(&mismatches)
	if err != nil {
		t.Fatal(err)
	}
	if mismatches != 0 {
		t.Fatalf("%d rows have stale scheduling columns", mismatches)
	}
}

func TestPostgresApplicationSchedulingColumns(t *testing.T) {
	ctx := context.Background()
	s := postgresTestStore(t)
	var triggers int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM pg_trigger WHERE tgrelid=$1::regclass AND NOT tgisinternal`, s.tableSQL).Scan(&triggers); err != nil {
		t.Fatal(err)
	}
	if triggers != 0 {
		t.Fatalf("found %d triggers", triggers)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	part := NewPart("job", "part", "bucket", "source", "finished", now)
	part.SourceArtifactBytes = ^uint64(0)
	if err := s.CreatePart(ctx, part); err != nil {
		t.Fatal(err)
	}
	assertSchedulingColumns(t, s)
	claimed, err := s.ClaimNextReady(ctx, "worker", now)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil {
		t.Fatal("no ready claim")
	}
	// Exercise partial updates with retained destination metadata and partitions.
	if _, err := s.updatePart(ctx, part.JobID, part.PartID, nil, func(p *Part) error {
		p.DestinationDatabase, p.DestinationTable, p.DestinationSchema = "db", "table", "schema"
		p.DestinationActivePartitionCounts = map[string]uint64{"p": 2}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, stats := range []*PartStats{{Count: 2, Bytes: ^uint64(0)}, nil, {}} {
		if err := s.UpdateRewriteProgress(ctx, part.JobID, part.PartID, "worker", RewriteProgress{DestinationActivePartStats: stats}, now); err != nil {
			t.Fatal(err)
		}
		assertSchedulingColumns(t, s)
	}
	if err := s.MarkCompactReady(ctx, part, "worker", "finished", "db", "table", "schema", PartStats{Count: 2, Bytes: 99}, map[string]uint64{"p": 2}, now); err != nil {
		t.Fatal(err)
	}
	assertSchedulingColumns(t, s)
	batch, err := s.ClaimNextCompactBatch(ctx, "worker", now, CompactClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("no compact claim")
	}
	assertSchedulingColumns(t, s)
	if _, err := s.HeartbeatCompactBatch(ctx, *batch, "worker", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertSchedulingColumns(t, s)
	if err := s.ReleaseCompactBatch(ctx, *batch, "worker", now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertSchedulingColumns(t, s)
	// A late heartbeat can reacquire an unowned ready part.
	if _, err := s.HeartbeatCompactBatch(ctx, *batch, "worker", now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertSchedulingColumns(t, s)
	output := batch.Parts[0]
	output.PartID, output.Status, output.WorkerID = "output", StatusCompactReady, ""
	output.CompactingAt = ""
	output.CompactGeneration = 1
	output.CompactInputPartIDs = []string{part.PartID}
	output.DestinationActivePartCount = 1
	output.DestinationActivePartitionCounts = map[string]uint64{"p": 1}
	if err := s.CompleteCompaction(ctx, *batch, output, "worker", now.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertSchedulingColumns(t, s)
	if n, err := s.FinalizeCompactReadyJob(ctx, part.JobID, time.Hour, now.Add(5*time.Minute)); err != nil || n != 1 {
		t.Fatalf("finalized %d: %v", n, err)
	}
	assertSchedulingColumns(t, s)
}

func TestPostgresSchedulingBackfill(t *testing.T) {
	ctx := context.Background()
	cfg := postgresTestConfig(t)
	s, err := openStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.pool.Close()
	if _, err := s.pool.Exec(ctx, s.migrations()[0]); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	for i, status := range []Status{StatusReady, StatusCompactReady, StatusCompacting, StatusCompactReady} {
		p := NewPart("legacy", fmt.Sprint(i), "bucket", "source", "finished", now)
		p.Status, p.SourceArtifactBytes = status, ^uint64(0)
		p.DestinationDatabase, p.DestinationTable, p.DestinationSchema = "db", "table", "schema"
		p.DestinationActivePartCount, p.DestinationActivePartBytes = 2, ^uint64(0)
		p.DestinationActivePartitionCounts = map[string]uint64{"p": 2, " ": 9, "empty": 0}
		p.CompactReadyAt = formatTime(now.Add(-time.Hour))
		if status == StatusCompacting {
			p.CompactingAt = formatTime(now.Add(-time.Minute))
		}
		if i == 3 {
			p.CompactGeneration = 1
			p.CompactInputPartIDs = []string{"input"}
			p.DestinationActivePartCount = 1
			p.DestinationActivePartitionCounts = map[string]uint64{"p": 1}
		}
		data, err := partJSON(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(ctx, `INSERT INTO `+s.tableSQL+` (job_id,part_id,status,worker_id,created_at,updated_at,data) VALUES ($1,$2,$3,$4,$5,$6,$7)`, p.JobID, p.PartID, string(p.Status), p.WorkerID, p.CreatedAt, p.UpdatedAt, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Migrate(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	assertSchedulingColumns(t, s)
}
