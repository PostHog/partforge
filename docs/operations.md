# Operations

Worker flags, metrics, and the admin/recovery commands. For deployment (ECS, IAM) see [deployment.md](deployment.md); for the internal rewrite/merge/compaction behavior see [rewrite-flow.md](rewrite-flow.md).

## Worker flags

- **`-role`** — `all` (default), `inserter`, or `compactor`. `all` claims `READY` rewrite work first and compacts when idle. `-compact=false` disables opportunistic compaction (valid only for `all`/`inserter`).
- **`-work-dir`** (default `/tmp/partforge`) — scratch root; put it on fast local disk with headroom (see [deployment.md](deployment.md)). Each claimed part gets its own `run-*` directory, removed when the part finishes.
- **`-once`** — process one unit of work and exit (used by the e2e script and for controlled draining).
- **`-poll-interval`** (default `10s`) — how long to wait before re-checking for work when idle.
- **`-compact-window`** (default `24h`) — the job-wide compaction period after the last original source artifact finishes rewriting; also the hard deadline for claimed compact merge waits. `0` finalizes as soon as no useful compaction remains.
- **`-insert-chunk-min-rows`** (default `10000000`) — minimum source rows per local insert chunk, up to 20 chunks per part. Parts below 20 million rows stay unsplit. `0` disables chunking for SQL requiring the whole source part. Completed chunks survive handled insert memory errors, but not worker/node loss. See [chunk semantics and recovery limits](rewrite-flow.md#local-insert-checkpoints).
- **`-default-compression-codec`** (default `ZSTD(5)`) — applied to the worker destination table before the insert-select.
- **`-clickhouse-binary`**, **`-clickhouse-config-file`**, **`-clickhouse-url`** — locate the local ClickHouse the worker starts.

The worker auto-tunes ClickHouse insert and merge settings from detected CPU/memory (memory-capped inserts and a ~150 GiB local merge target), while compactor workers run one merge at a time. The derivation and the merge-wait state machine are documented in [rewrite-flow.md](rewrite-flow.md).

## Metrics

`partforge worker` serves Prometheus metrics on `:2112/metrics` by default. Use `-metrics-addr=""` to disable, or `-metrics-addr` / `-metrics-path` to change where it listens.

Core metrics:

- `partforge_rows_read_total`, `partforge_bytes_read_total`
- `partforge_rows_written_total`, `partforge_bytes_written_total`
- `partforge_current_read_rows`, `partforge_current_read_bytes`, `partforge_current_total_rows_approx`, `partforge_current_written_rows`, `partforge_current_written_bytes`
- `partforge_active_part_count`, `partforge_active_part_rows`, `partforge_active_part_bytes`
- `partforge_forges_started_total`, `partforge_forges_completed_total`, `partforge_forges_failed_total`
- `partforge_compact_batch_active`, `partforge_compact_stage`, `partforge_compact_active_merges`
- `partforge_compact_part_count`, `partforge_compact_part_rows`, `partforge_compact_part_bytes`
- `partforge_compact_partition_parts`, `partforge_compact_partition_rows`, `partforge_compact_partition_bytes`
- `partforge_compact_merge_progress_ratio`, `partforge_compact_merge_elapsed_seconds`, `partforge_compact_merge_source_parts`
- `partforge_compact_merge_rows_read`, `partforge_compact_merge_rows_total`, `partforge_compact_merge_bytes_read`, `partforge_compact_merge_bytes_total`

Read/write counters and ClickHouse's estimated total rows to read update live while the `INSERT SELECT` runs, polled from the local ClickHouse `system.processes` for the rewrite query id. Divide `partforge_current_read_rows` by `partforge_current_total_rows_approx` in PromQL for an estimated progress ratio. Active-part gauges come from `system.parts` while those parts are attached. During compaction, the worker independently polls `system.parts` and `system.merges` while ClickHouse selects and runs background merges. A native merge is identified by `job_id`, the stable compact `output_part_id`, `partition_id`, and ClickHouse `result_part_name`. Compact gauges are removed when the batch ends so finished batches do not remain in live Grafana totals.

Workers also write a per-part progress heartbeat to Postgres every `15s` (`-state-progress-interval`, `0` disables) so `job-status` reflects progress even during S3 transfer stages.

### Insert retry cost

Three metric families measure the insert-select retry loop:

| Metric | Meaning |
| --- | --- |
| `partforge_insert_attempt_duration_seconds` | Histogram per attempt, labeled `result="completed"` or `"failed"`. Its `_count` counts attempts; `_sum` measures their wall time, including progress reporting. Excludes time between attempts. |
| `partforge_insert_duration_seconds` | Histogram per part's insert loop, including all attempts, table resets, codec setup, and backoff. Labels: `result` and `retried="true"` or `"false"` (whether a second attempt actually started). |
| `partforge_insert_wasted_seconds_total` | Time outside the successful attempt for retried inserts. First-attempt success adds zero. A terminal failure or cancellation adds the entire loop duration, including interrupted recovery. Same labels as insert duration. |

All three include `job_id`, `source_table`, and `destination_table`; scraped `pod` and `environment` labels allow per-worker breakdowns. Attempts update when each attempt ends. Whole-loop duration and wasted time update together when the loop exits, even on error. Running attempts and abrupt worker deaths are not included until/unless their observation executes. These are cumulative PartForge process metrics and survive local ClickHouse restarts; worker restarts reset them. Use `increase` before summing across workers. Very short-lived processes or events before the first scrape can be missed.

The `insert-select summary` log records `job_id`, `part_id`, `result`, `attempts`, `elapsed`, `wasted_elapsed`, `successful_attempt_elapsed`, and the final thread/block settings. Retry warnings also record `attempt_elapsed` and the error. This gives per-part detail without a metric series for every part or setting value.

Use these expressions in Grafana Explore, replacing `JOB_ID` (add an `environment` filter if needed):

```promql
# Failed attempts in the last hour, including failures eventually recovered.
sum(increase(partforge_insert_attempt_duration_seconds_count{job_id="JOB_ID",result="failed"}[1h]))

# Percentage of finished insert loops that needed at least one retry.
100 * sum(increase(partforge_insert_duration_seconds_count{job_id="JOB_ID",retried="true"}[1h]))
/ sum(increase(partforge_insert_duration_seconds_count{job_id="JOB_ID"}[1h]))

# Wasted worker-hours, split between recovered inserts and terminal failures.
sum by (result) (increase(partforge_insert_wasted_seconds_total{job_id="JOB_ID"}[1h])) / 3600

# Actual average insertion seconds per successfully inserted part, retries included.
sum(increase(partforge_insert_duration_seconds_sum{job_id="JOB_ID",result="completed"}[1h]))
/ sum(increase(partforge_insert_duration_seconds_count{job_id="JOB_ID",result="completed"}[1h]))

# Time multiplier from retries on successfully inserted parts: 1 = no overhead, 3 = 3x.
sum(increase(partforge_insert_duration_seconds_sum{job_id="JOB_ID",result="completed"}[1h]))
/ (
  sum(increase(partforge_insert_duration_seconds_sum{job_id="JOB_ID",result="completed"}[1h]))
  - sum(increase(partforge_insert_wasted_seconds_total{job_id="JOB_ID",result="completed"}[1h]))
)
```

The estimated average without retry waste is the actual average divided by that multiplier. For example, two failed attempts totaling 17s, 3s of recovery/backoff, and a 10s successful attempt produce 30s actual, 20s wasted, and a 3x multiplier. The baseline is the successful attempt on those same parts at their final settings; it is not a prediction of how fast different settings would run. These figures cover insertion only, exclude download/merge/upload, and sum worker wall time rather than CPU time or job elapsed time. Empty windows leave ratios undefined rather than claiming zero overhead.

Each part starts from the original insert settings; reductions only apply within that part's retry loop. A high retried-part percentage together with a large multiplier is evidence to investigate initial settings using the logged errors and final settings. A high failure count alone does not establish high cost: early failures may be cheap. Compare similar parts and jobs when evaluating a configuration change; part sizes vary, and a later manual requeue is a separate insert loop.

## Inspecting jobs

```sh
partforge list-jobs                 # jobs with status, part counts, submitted/updated timestamps, optional names
partforge job-status -job-id=job-123
```

Both accept `-json`; `list-jobs -json` keeps `jobs` as job IDs, adds `job_names` when names are set, and includes `job_details` for status/progress/timestamps. `job-status` lists each active rewrite with its stage, elapsed time, rows read, ClickHouse-estimated total rows, and select progress, followed by active compacting batches. The select percentage can decrease when ClickHouse revises its estimate upward. `job-status -parts` adds per-row detail (persisted rewrite and compaction counters, compact-ready age, destination partitions, active part stats, `FAILED_MERGES`); `job-status -details` adds each part's current rewrite stage and per-stage timings. The physical part counters (`input_clickhouse_parts`, `current_output_clickhouse_parts`) refer to ClickHouse parts, not state rows.

## Admin and recovery commands

All take `-job-id`. Most use conditional updates and take `-force` where a guard would otherwise block them.

| Command | What it does |
| --- | --- |
| `retry-failed` | Move failed parts back to their retryable state. `-part-id` / `-all`; `-include-in-progress` also resets stuck workers; `-stale` (with `-stale-after`, default `5m`) resets only in-progress parts with no recent progress; `-force` re-runs even completed parts. |
| `set-part-state` | Force selected rows to a stable state (`READY`, `COMPACT_READY`, or `FINISHED`) and clear stale ownership. Select by repeated `-part-id` or by `-status`. |
| `finalize-compaction` | Ask compacting workers to save current useful output and finish. |
| `reset-compact-timer` | Restart the job's compact-window timer (sets `compact_ready_at` to now on every row). |
| `reset-job` | Delete generated compact rows and move originals back to `READY` (full re-rewrite). `-delete-s3` also removes generated + rewritten artifacts (keeps uploaded `source/`). |
| `reset-compaction` | Delete generated compact rows and move rewritten originals back to `COMPACT_READY` (re-compact only). `-delete-s3` removes generated compact artifacts. |
| `delete-parts` | Force-delete selected Postgres state rows only — never touches S3 or already-attached data. |
| `delete-job` | Delete a job's Postgres state rows; `-delete-s3` also deletes `s3://bucket/<prefix>/jobs/<job-id>/*`. |
| `version` | Print the build version. |

Notes:

- `retry-failed` moves failed rewrite parts back to `READY` and failed import parts back to `FINISHED` (so `import-finished` retries the import stage without re-running the worker). Any move back to `READY` clears persisted rewrite progress and metrics.
- `reset-job` and `reset-compaction` validate compaction lineage (`compact_input_part_ids` / `superseded_by`) and refuse to run if any part has started import.
- `-delete-s3` variants derive the exact S3 target from the job's recorded rows and reject glob metacharacters before deleting. For jobs created with `upload-freeze -copy-parts-from-job`, borrowed source prefixes are not deleted; jobs that own referenced source parts are blocked from deletion while those references exist.

## Shutdown behavior

On `SIGINT`/`SIGTERM` a worker stops claiming new work immediately. An active insert is canceled and its part returned to `READY`. Active compaction stops waiting for more merge progress, then uploads its output only if it reduced the physical part count, otherwise releases the batch back to `COMPACT_READY`. If a worker process dies outside handled code, the part stays visible as `IN_PROGRESS` or `COMPACTING` for manual inspection or reset (`set-part-state` / `retry-failed -include-in-progress`).
