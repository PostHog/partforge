# Postgres state store

PartForge tracks every part of every job in Postgres. This is the source of truth for the pipeline: workers claim work with row locks, transition parts through their lifecycle, and record progress and compaction lineage here. Because all state lives in Postgres, jobs are resumable and can be driven by many workers at once.

The table holds no bulk data, only per-part metadata and pointers to S3 artifacts. Part data itself lives in S3. Rows may include `job_name` when `upload-freeze -job-name` is used; `list-jobs` displays it with job status, counts, and timestamps.

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

## Schema

The app creates the table and indexes on startup:

```sql
CREATE TABLE IF NOT EXISTS partforge_state (
    job_id text NOT NULL,
    part_id text NOT NULL,
    status text NOT NULL,
    worker_id text NOT NULL DEFAULT '',
    created_at text NOT NULL,
    updated_at text NOT NULL,
    data jsonb NOT NULL,
    PRIMARY KEY (job_id, part_id)
);

CREATE INDEX IF NOT EXISTS partforge_state_status_idx
    ON partforge_state (status, created_at, job_id, part_id);

CREATE INDEX IF NOT EXISTS partforge_state_job_status_idx
    ON partforge_state (job_id, status, part_id);
```

The scalar columns support claims and job/status scans. The full part record is stored in `data`.

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
