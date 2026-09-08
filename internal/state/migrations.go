package state

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Migrations are append-only. Each state table has its own version ledger.
func (s *Store) migrations() []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
 job_id text NOT NULL, part_id text NOT NULL, status text NOT NULL,
 worker_id text NOT NULL DEFAULT '', created_at text NOT NULL,
 updated_at text NOT NULL, data jsonb NOT NULL, PRIMARY KEY (job_id, part_id));
 CREATE INDEX IF NOT EXISTS %s ON %s (status, created_at, job_id, part_id);
 CREATE INDEX IF NOT EXISTS %s ON %s (job_id, status, part_id);`,
			s.tableSQL, s.statusIndexSQL, s.tableSQL, s.jobStatusIndexSQL, s.tableSQL),
		fmt.Sprintf(`ALTER TABLE %[1]s ADD COLUMN source_artifact_bytes numeric(20,0) NOT NULL DEFAULT 0;
 UPDATE %[1]s SET source_artifact_bytes = COALESCE((data->>'source_artifact_bytes')::numeric, 0);
 ALTER TABLE %[1]s ADD CHECK (source_artifact_bytes >= 0);
 CREATE INDEX %[2]s ON %[1]s (source_artifact_bytes DESC, created_at, job_id, part_id) WHERE status = 'READY';`,
			s.tableSQL, s.indexSQL("ready_priority_idx")),
		fmt.Sprintf(`ALTER TABLE %[1]s
 ADD COLUMN compact_bytes numeric(20,0) NOT NULL DEFAULT 0,
 ADD COLUMN compact_eligible boolean NOT NULL DEFAULT false,
 ADD COLUMN compact_normalized boolean NOT NULL DEFAULT false,
 ADD COLUMN compact_stale_at timestamptz,
 ADD COLUMN original_compact_ready_at timestamptz;
 UPDATE %[1]s SET
 original_compact_ready_at = CASE WHEN COALESCE((data->>'compact_generation')::int, 0) <= 0 AND jsonb_array_length(COALESCE(NULLIF(data->'compact_input_part_ids', 'null'::jsonb), '[]'::jsonb)) = 0 THEN NULLIF(data->>'compact_ready_at', '')::timestamptz END,
 compact_bytes = COALESCE((data->>'destination_active_part_bytes')::numeric, 0),
 compact_eligible =
 COALESCE(btrim(data->>'destination_database'), '') <> '' AND
 COALESCE(btrim(data->>'destination_table'), '') <> '' AND
 COALESCE(btrim(data->>'destination_schema'), '') <> '' AND
 COALESCE((data->>'destination_active_part_count')::numeric, 0) > 0 AND
 EXISTS (SELECT FROM jsonb_each_text(COALESCE(NULLIF(data->'destination_active_partition_counts', 'null'::jsonb), '{}'::jsonb)) p WHERE btrim(p.key) <> '' AND p.value::numeric > 1),
 compact_normalized = COALESCE((data->>'destination_active_part_count')::numeric, 0) = 1 AND
 (SELECT count(*) = 1 AND COALESCE(bool_and(p.value::numeric = 1), false)
 FROM jsonb_each_text(COALESCE(NULLIF(data->'destination_active_partition_counts', 'null'::jsonb), '{}'::jsonb)) p WHERE btrim(p.key) <> '' AND p.value::numeric > 0),
 compact_stale_at = CASE WHEN status = 'COMPACTING' THEN
 LEAST(NULLIF(updated_at, '')::timestamptz, NULLIF(data->>'compacting_at', '')::timestamptz) END;
 ALTER TABLE %[1]s ADD CHECK (status <> 'COMPACTING' OR compact_stale_at IS NOT NULL);
 CREATE INDEX %[2]s ON %[1]s (compact_bytes DESC, created_at, job_id, part_id) WHERE status = 'COMPACT_READY' AND compact_eligible;
 CREATE INDEX %[3]s ON %[1]s (compact_stale_at, job_id, part_id) WHERE status = 'COMPACTING';
 CREATE INDEX %[4]s ON %[1]s (job_id, original_compact_ready_at DESC) WHERE original_compact_ready_at IS NOT NULL;
 CREATE TABLE %[5]s (id boolean PRIMARY KEY CHECK (id), next_run_at timestamptz NOT NULL);
 INSERT INTO %[5]s VALUES (true, '-infinity');`,
			s.tableSQL, s.indexSQL("compact_priority_idx"), s.indexSQL("compact_stale_idx"), s.indexSQL("compact_deadline_idx"), s.relatedSQL("maintenance")),
	}
}

func (s *Store) indexSQL(suffix string) string { return quoteIndexName(s.tableName, suffix) }

func (s *Store) relatedSQL(suffix string) string {
	parts := strings.Split(s.tableName, ".")
	name := quoteIndexName(parts[len(parts)-1], suffix)
	if len(parts) > 1 {
		return pgx.Identifier(parts[:len(parts)-1]).Sanitize() + "." + name
	}
	return name
}

func (s *Store) schemaVersion(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}) (int, error) {
	rows, err := q.Query(ctx, `SELECT version FROM `+s.relatedSQL("migrations")+` ORDER BY version`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	version := 0
	for rows.Next() {
		var next int
		if err := rows.Scan(&next); err != nil {
			return 0, err
		}
		if next != version+1 || next > len(s.migrations()) {
			return 0, fmt.Errorf("unsupported migration history: version %d after %d", next, version)
		}
		version = next
	}
	return version, rows.Err()
}

func (s *Store) checkSchema(ctx context.Context) error {
	version, err := s.schemaVersion(ctx, s.pool)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
		return errors.New("state schema is not migrated; run partforge migrate with the same Postgres URL and state table")
	}
	if err != nil {
		return fmt.Errorf("check state schema: %w", err)
	}
	if version != len(s.migrations()) {
		return fmt.Errorf("state schema is at version %d, expected %d; run partforge migrate", version, len(s.migrations()))
	}
	return nil
}

// Migrate adopts the original hand-created schema and applies pending upgrades.
// Stop workers for migration: backfills and index builds hold table locks.
func Migrate(ctx context.Context, cfg Config) (int, error) {
	s, err := openStore(ctx, cfg)
	if err != nil {
		return 0, err
	}
	defer s.pool.Close()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	// ponytail: serialize migrations per database; per-table locks if parallel migrations are ever needed.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('partforge:migrate', 0))`); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+s.relatedSQL("migrations")+` (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return 0, err
	}
	version, err := s.schemaVersion(ctx, tx)
	if err != nil {
		return 0, err
	}
	migrations := s.migrations()
	for i := version; i < len(migrations); i++ {
		if _, err := tx.Exec(ctx, migrations[i]); err != nil {
			return 0, fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+s.relatedSQL("migrations")+` (version) VALUES ($1)`, i+1); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(migrations) - version, nil
}
