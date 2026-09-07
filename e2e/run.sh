#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT/.e2e/clickhouse-data"
CH_HTTP_HOST="http://127.0.0.1:18123"
CH_HTTP_DOCKER="http://clickhouse:8123"
POSTGRES_URL="postgres://partforge:partforge@postgres:5432/partforge?sslmode=disable"
JOB_ID="e2e-job"
COPY_JOB_ID="e2e-copy-job"
BACKUP_JOB_ID="e2e-backup-job"
BACKUP_COPY_JOB_ID="e2e-backup-copy-job"
JOB_NAME="E2E import"

cd "$ROOT"

log_value() {
  local line="$1"
  local key="$2"
  printf '%s\n' "$line" |
    tr ' ' '\n' |
    sed -n "s/^${key}=//p" |
    tail -n 1 |
    tr -d '"'
}

require_uint() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "expected numeric $name in worker settings log, got ${value:-<empty>}" >&2
    exit 1
  fi
}

assert_worker_insert_memory_settings() {
  local log_file="$1"
  local line
  line="$(grep 'configured clickhouse resource settings' "$log_file" | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    echo "worker log $log_file did not contain configured clickhouse resource settings" >&2
    exit 1
  fi

  local cpus memory_bytes max_threads max_insert_threads max_memory_usage
  cpus="$(log_value "$line" "cpus")"
  memory_bytes="$(log_value "$line" "memory_bytes_raw")"
  max_threads="$(log_value "$line" "max_threads")"
  max_insert_threads="$(log_value "$line" "max_insert_threads")"
  max_memory_usage="$(log_value "$line" "max_memory_usage_raw")"

  require_uint "cpus" "$cpus"
  require_uint "memory_bytes" "$memory_bytes"
  require_uint "max_threads" "$max_threads"
  require_uint "max_insert_threads" "$max_insert_threads"
  require_uint "max_memory_usage" "$max_memory_usage"

  local cpu_threads expected_threads
  if (( cpus < 2 )); then
    cpu_threads=1
  else
    cpu_threads=$((cpus / 2))
  fi
  expected_threads=$cpu_threads
  local expected_max_memory
  expected_max_memory=$((memory_bytes * 70 / 100))

  if (( max_threads != expected_threads )); then
    echo "max_threads=$max_threads, expected $expected_threads from cpus=$cpus" >&2
    exit 1
  fi
  if (( max_insert_threads != expected_threads )); then
    echo "max_insert_threads=$max_insert_threads, expected $expected_threads from cpus=$cpus" >&2
    exit 1
  fi
  if (( max_memory_usage != expected_max_memory )); then
    echo "max_memory_usage=$max_memory_usage, expected $expected_max_memory from memory_bytes=$memory_bytes" >&2
    exit 1
  fi
}

docker compose down --remove-orphans >/dev/null 2>&1 || true
rm -rf "$ROOT/.e2e"
mkdir -p "$DATA_DIR"
chmod -R a+rwx "$ROOT/.e2e"

if [[ "${PARTFORGE_E2E_SKIP_BUILD:-}" != "1" ]]; then
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose build worker
fi
CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose up -d localstack postgres clickhouse

for _ in $(seq 1 60); do
  if curl -fsS "$CH_HTTP_HOST/?query=SELECT%201" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "$CH_HTTP_HOST/?query=SELECT%201" >/dev/null

for _ in $(seq 1 60); do
  if docker compose exec -T localstack awslocal s3 ls >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker compose exec -T localstack awslocal s3 mb s3://partforge >/dev/null 2>&1 || true
printf '1\n' | docker compose exec -T localstack awslocal s3 cp - s3://partforge/e2e-s3-credentials.tsv >/dev/null

for _ in $(seq 1 60); do
  if docker compose exec -T postgres pg_isready -U partforge -d partforge >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker compose exec -T postgres pg_isready -U partforge -d partforge >/dev/null

docker compose exec -T clickhouse clickhouse-client --multiquery < e2e/sql/setup_and_freeze.sql

docker compose exec -T clickhouse clickhouse-client --query \
  "BACKUP TABLE src.events TO S3('http://localstack:4566/partforge/e2e-native-backup', 'test', 'test')"

docker compose exec -T clickhouse clickhouse-client --query \
  "INSERT INTO src.events VALUES (5, 'echo', 50, '2024-01-05')"

docker compose exec -T clickhouse clickhouse-client --query \
  "BACKUP TABLE src.events TO S3('http://localstack:4566/partforge/e2e-native-incremental', 'test', 'test') SETTINGS base_backup = S3('http://localstack:4566/partforge/e2e-native-backup', 'test', 'test')"

# Production backup metadata uses s3:// base locators. ClickHouse preserves the
# LocalStack HTTP endpoint and credentials, so normalize only this test index.
docker compose exec -T localstack awslocal s3 cp \
  s3://partforge/e2e-native-incremental/.backup /tmp/e2e-native-incremental.backup >/dev/null
docker compose exec -T localstack sed -i \
  "s#S3('http://localstack:4566/partforge/e2e-native-backup', 'test', 'test')#S3('s3://partforge/e2e-native-backup')#" \
  /tmp/e2e-native-incremental.backup
docker compose exec -T localstack grep -F \
  "<base_backup>S3('s3://partforge/e2e-native-backup')</base_backup>" \
  /tmp/e2e-native-incremental.backup >/dev/null
docker compose exec -T localstack awslocal s3 cp \
  /tmp/e2e-native-incremental.backup s3://partforge/e2e-native-incremental/.backup >/dev/null

clickhouse_owner="$(docker compose exec -T clickhouse stat -c '%u:%g' /var/lib/clickhouse)"

part_count="$(
  docker compose exec -T -u "$clickhouse_owner" clickhouse \
    find /var/lib/clickhouse -path "*/shadow/e2e_freeze/*/checksums.txt" |
    wc -l |
    tr -d ' '
)"
if [[ "$part_count" == "0" ]]; then
  echo "no frozen parts found" >&2
  exit 1
fi

incremental_part_count="$(
  docker compose exec -T clickhouse clickhouse-client --query \
    "SELECT countDistinct(_part) FROM src.events"
)"

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm \
  --workdir /work \
  -v "$ROOT:/work:ro" \
  worker \
  upload-backup \
  -backup=s3://partforge/e2e-native-incremental \
  -database=src \
  -table=events \
  -destination-schema-file=e2e/sql/destination.sql \
  -insert-select-file=e2e/sql/insert.sql \
  -bucket=partforge \
  -prefix=e2e \
  -job-id="$BACKUP_JOB_ID" \
  -zero-copy \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL"

backup_part_count="$(
  docker compose exec -T postgres psql -U partforge -d partforge -Atc \
    "SELECT count(*) FROM partforge_state WHERE job_id = '$BACKUP_JOB_ID'"
)"
if [[ "$backup_part_count" != "$incremental_part_count" ]]; then
  echo "incremental backup parts=$backup_part_count, expected $incremental_part_count" >&2
  exit 1
fi

manifest_count="$(
  docker compose exec -T localstack awslocal s3 ls \
    "s3://partforge/e2e/jobs/$BACKUP_JOB_ID/source/" --recursive |
    awk '$4 ~ /\/manifest.json$/ { count++ } END { print count + 0 }'
)"
if [[ "$manifest_count" != "$incremental_part_count" ]]; then
  echo "zero-copy manifests=$manifest_count, expected $incremental_part_count" >&2
  exit 1
fi
if docker compose exec -T localstack awslocal s3 ls \
  "s3://partforge/e2e/jobs/$BACKUP_JOB_ID/source/" --recursive |
  grep -E '/(checksums|columns)\.txt$' >/dev/null; then
  echo "zero-copy job unexpectedly copied source part data" >&2
  exit 1
fi

for i in $(seq 1 "$incremental_part_count"); do
  backup_worker_log="$ROOT/.e2e/backup-worker-${i}.log"
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
    worker \
    -role=inserter \
    -merge-max-runtime=1ns \
    -s3-endpoint=http://localstack:4566 \
    -postgres-url="$POSTGRES_URL" \
    -once 2>&1 | tee "$backup_worker_log"
  if ! grep -F "downloading zero-copy source objects" "$backup_worker_log" >/dev/null; then
    echo "backup worker did not resolve zero-copy source pointers" >&2
    exit 1
  fi
done

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
  delete-job \
  -job-id="$BACKUP_JOB_ID" \
  -delete-s3 \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL"

docker compose exec -T localstack awslocal s3 ls \
  s3://partforge/e2e-native-backup/.backup >/dev/null
docker compose exec -T localstack awslocal s3 ls \
  s3://partforge/e2e-native-incremental/.backup >/dev/null

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm \
  --workdir /work \
  -v "$ROOT:/work:ro" \
  worker \
  upload-backup \
  -backup=s3://partforge/e2e-native-backup \
  -database=src \
  -table=events \
  -destination-schema-file=e2e/sql/destination.sql \
  -insert-select-file=e2e/sql/insert.sql \
  -bucket=partforge \
  -prefix=e2e \
  -job-id="$BACKUP_COPY_JOB_ID" \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL"

for file in checksums.txt columns.txt; do
  copied_count="$(
    docker compose exec -T localstack awslocal s3 ls \
      "s3://partforge/e2e/jobs/$BACKUP_COPY_JOB_ID/source/" --recursive |
      awk -v suffix="/$file" '$4 ~ suffix "$" { count++ } END { print count + 0 }'
  )"
  if [[ "$copied_count" != "$part_count" ]]; then
    echo "materialized $file objects=$copied_count, expected $part_count" >&2
    exit 1
  fi
done

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
  delete-job \
  -job-id="$BACKUP_COPY_JOB_ID" \
  -delete-s3 \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL"

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm --user "$clickhouse_owner" \
  --workdir /work \
  -v "$ROOT:/work:ro" \
  -v "$DATA_DIR:/var/lib/clickhouse" \
  worker \
  upload-freeze \
  -database=src \
  -table=events \
  -freeze=e2e_freeze \
  -destination-schema-file=e2e/sql/destination.sql \
  -insert-select-file=e2e/sql/insert.sql \
  -clickhouse-url="$CH_HTTP_DOCKER" \
  -bucket=partforge \
  -prefix=e2e \
  -job-id="$JOB_ID" \
  -job-name="$JOB_NAME" \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL"

job_list="$(
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
    list-jobs \
    -postgres-url="$POSTGRES_URL"
)"
if ! grep -F "E2E import" <<<"$job_list" >/dev/null; then
  echo "list-jobs did not include job name; output:" >&2
  echo "$job_list" >&2
  exit 1
fi

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm --user "$clickhouse_owner" \
  --workdir /work \
  -v "$ROOT:/work:ro" \
  worker \
  upload-freeze \
  -copy-parts-from-job="$JOB_ID" \
  -destination-schema-file=e2e/sql/destination.sql \
  -insert-select-file=e2e/sql/insert.sql \
  -bucket=partforge \
  -prefix=e2e \
  -job-id="$COPY_JOB_ID" \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL"

if CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
  delete-job \
  -job-id="$JOB_ID" \
  -delete-s3 \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL"; then
  echo "delete-job unexpectedly deleted a source job with copied source references" >&2
  exit 1
fi

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
  delete-job \
  -job-id="$COPY_JOB_ID" \
  -postgres-url="$POSTGRES_URL"

largest_source_part_id="$(
  docker compose exec -T postgres psql -U partforge -d partforge -Atc \
    "SELECT part_id FROM partforge_state WHERE job_id = '$JOB_ID' ORDER BY COALESCE((data->>'source_artifact_bytes')::bigint, 0) DESC, created_at, job_id, part_id LIMIT 1"
)"
if [[ -z "$largest_source_part_id" ]]; then
  echo "could not determine largest source part" >&2
  exit 1
fi

for i in $(seq 1 "$part_count"); do
  worker_log="$ROOT/.e2e/worker-${i}.log"
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
    worker \
    -role=inserter \
    -merge-max-runtime=1ns \
    -s3-endpoint=http://localstack:4566 \
    -postgres-url="$POSTGRES_URL" \
    -once 2>&1 | tee "$worker_log"
  assert_worker_insert_memory_settings "$worker_log"
  if (( i == 1 )) && ! grep -F "claimed ready part" "$worker_log" | grep -F "part_id=$largest_source_part_id" >/dev/null; then
    echo "first worker did not claim largest source part $largest_source_part_id" >&2
    exit 1
  fi
done

normalized_finalize_log="$ROOT/.e2e/compact-normalized-finalize.log"
CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
  worker \
  -role=compactor \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL" \
  -compact-window=1h \
  -once 2>&1 | tee "$normalized_finalize_log"

if ! grep -F "finalized compact-ready artifacts" "$normalized_finalize_log" >/dev/null; then
  echo "expected compact worker to finalize normalized artifact" >&2
  exit 1
fi

compact_log="$ROOT/.e2e/compact-merge.log"
CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
  worker \
  -role=compactor \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL" \
  -compact-window=1h \
  -once 2>&1 | tee "$compact_log"

if ! grep -F "claimed compact-ready batch" "$compact_log" >/dev/null; then
  echo "expected compact worker to claim fragmented artifact" >&2
  exit 1
fi
if ! grep -F "completed compact batch" "$compact_log" >/dev/null; then
  echo "expected compact worker to complete fragmented artifact" >&2
  exit 1
fi

compact_status="$(
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
    job-status \
    -job-id="$JOB_ID" \
    -parts \
    -postgres-url="$POSTGRES_URL"
)"
if ! grep -E '^compact-[^[:space:]]+[[:space:]]+COMPACT_READY' <<<"$compact_status" >/dev/null; then
  echo "job-status did not contain compact checkpoint; output:" >&2
  echo "$compact_status" >&2
  exit 1
fi

finalize_log="$ROOT/.e2e/compact-finalize.log"
CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
  worker \
  -role=compactor \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL" \
  -compact-window=0s \
  -once 2>&1 | tee "$finalize_log"

status="$(
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
    job-status \
    -job-id="$JOB_ID" \
    -postgres-url="$POSTGRES_URL" |
    sed -n 's/^status: //p'
)"

if [[ "${status:-}" != "READY_FOR_IMPORT" ]]; then
  echo "job did not reach READY_FOR_IMPORT; status=${status:-<empty>}" >&2
  exit 1
fi
if ! grep -F "finalized compact-ready artifacts" "$finalize_log" >/dev/null; then
  echo "expected compact worker to finalize normalized artifacts" >&2
  exit 1
fi

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm --user "$clickhouse_owner" \
  -v "$DATA_DIR:/var/lib/clickhouse" \
  worker \
  import-finished \
  -database=dst \
  -table=events_new \
  -job-id="$JOB_ID" \
  -clickhouse-url="$CH_HTTP_DOCKER" \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL"

docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT id, name, amount_text, event_date, migrated FROM dst.events_new ORDER BY id FORMAT TSV" \
  > "$ROOT/.e2e/actual.tsv"

diff -u e2e/expected.tsv "$ROOT/.e2e/actual.tsv"

output_part_count="$(docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT count() FROM system.parts WHERE database = 'dst' AND table = 'events_new' AND active")"
if [[ "$output_part_count" != "$part_count" ]]; then
  echo "output ClickHouse parts=$output_part_count, expected uploaded parts=$part_count" >&2
  exit 1
fi

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
  delete-job \
  -job-id="$JOB_ID" \
  -delete-s3 \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL"

echo "e2e passed with $part_count frozen parts"
