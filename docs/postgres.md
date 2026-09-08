# Postgres state store

PartForge tracks every part of every job in Postgres. This is the source of truth for the pipeline: workers claim work with row locks, transition parts through their lifecycle, and record progress and compaction lineage here. Because all state lives in Postgres, jobs are resumable and can be driven by many workers at once.

The table holds no bulk data, only per-part metadata and pointers to S3 artifacts. Part data itself lives in S3. Rows may include `job_name` when `upload-freeze -job-name` is used; `list-jobs` displays it with job status, counts, and timestamps. Rows created with `upload-freeze -copy-parts-from-job` store `source_job_id` and `source_part_id` in `data` so the source owner rows cannot be deleted while copied jobs still reference their uploaded source artifacts.

## Connection

Pass the state database as `-postgres-url` or `postgres_url` in config:

```sh
-postgres-url='postgres://partforge@partforge.cluster-abc.us-east-1.rds.amazonaws.com:5432/partforge?sslmode=require'
```

For local compose runs:

```sh
-postgres-url='postgres://partforge:partforge@localhost:15432/partforge?sslmode=disable'
```

Default table name is `partforge_state`; override with `-state-table` or `state_table`.

## Migrate before starting workers

```sh
partforge migrate -postgres-url="$POSTGRES_URL"
```

Use the same `-config`, `-state-table`, `-postgres-iam-auth`, and `-aws-region` settings as the workers. A schema-qualified state table is supported. The database role running migrations must own the existing state table and have permission to create tables and indexes in its schema.

For an existing deployment:

1. Stop workers and pause uploads and state-changing admin commands.
2. Run `partforge migrate` with the new binary against the existing state table.
3. Start the new workers after the command succeeds.

Migrations backfill columns and build indexes in one transaction, holding table locks. Plan a maintenance window; this is not an online migration command. Existing rows, statuses, progress, and lineage are retained. The first migration adopts the original hand-created table with `CREATE TABLE IF NOT EXISTS`; do not recreate or empty it.

The command records numbered versions in `<state-table>_migrations`. An advisory transaction lock serializes concurrent migration commands, and a failure rolls back the pending changes and their version records together. Re-running a successful migration command is a no-op. Unexpected versions or gaps fail explicitly. Workers check the version at startup and tell you to run `partforge migrate` if an upgrade is needed; they no longer execute schema DDL.

Migrations are compiled into the binary in [internal/state/migrations.go](../internal/state/migrations.go). Append new SQL migrations to the list; never edit, reorder, or remove an already released migration. There is no automatic downgrade command.

## Schema and scheduling

The original primary key `(job_id, part_id)`, state columns, and full `data` JSON record remain. Additional scalar columns support scheduling without indexing the mutable JSON itself:

| Columns | Purpose |
|---|---|
| `source_artifact_bytes` | Ordered partial index for largest-first READY claims |
| `compact_bytes`, `compact_eligible` | Ordered partial index for eligible compact claims |
| `compact_normalized` | Identify artifacts containing one physical part |
| `compact_stale_at` | Indexed stale-compaction lookup, preserving the existing earlier-of-heartbeat-and-claim timeout |
| `original_compact_ready_at` | Indexed job compact deadline lookup |

Application writes update these projections in the same SQL statement as the JSON or status change. No triggers are installed. Use PartForge commands for state changes; direct SQL must maintain the corresponding derived columns too. The full Part record remains in `data`; no job metadata or progress table is required.

Compactors claim the largest eligible unlocked artifact. Job, destination, and explicit partition filters still apply; partitions with an active compactor are no longer deprioritized.

A single `<state-table>_maintenance` record reserves compaction maintenance once every ten seconds across the worker fleet. `worker -once` requests an immediate pass but still skips a pass already in progress. Only the reservation holder scans job summaries and expires stale work. The reservation commits with the work, and active maintenance is skipped by other workers without waiting. Workers sharing a state table should use the same compact-window and stale-timeout settings. New finalizable output can wait up to the next maintenance pass; a long maintenance run or database contention can extend that delay.

Rewrite progress combines live query counters and stage timing in one periodic heartbeat. Rewrite and compact progress updates use conditional SQL patches that check worker ownership. A failed rewrite heartbeat cancels processing and surfaces the error.

Connection pool configuration is unchanged.

## IAM Auth

For RDS or Aurora PostgreSQL, enable IAM database authentication on the cluster, then create a database role that maps to the IAM principal:

```sql
CREATE USER partforge;
GRANT rds_iam TO partforge;
GRANT CONNECT ON DATABASE partforge TO partforge;
GRANT USAGE, CREATE ON SCHEMA public TO partforge;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO partforge;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO partforge;
```

Run commands with:

```sh
-postgres-url='postgres://partforge@partforge.cluster-abc.us-east-1.rds.amazonaws.com:5432/partforge?sslmode=require' \
-postgres-iam-auth \
-aws-region=us-east-1
```

Grant the task or instance role `rds-db:connect` for that database user:

```json
{
  "Effect": "Allow",
  "Action": "rds-db:connect",
  "Resource": "arn:aws:rds-db:us-east-1:123456789012:dbuser:db-ABCDEFGHIJKLMNOP/partforge"
}
```

The resource uses the RDS DB resource ID, not the cluster identifier. Keep normal S3 permissions separate; see [deployment.md](deployment.md).
