#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT/.e2e/clickhouse-data"
CH_HTTP_HOST="http://127.0.0.1:18123"
CH_HTTP_DOCKER="http://clickhouse:8123"
POSTGRES_URL="postgres://partforge:partforge@postgres:5432/partforge?sslmode=disable"
JOB_ID="e2e-job"
COPY_JOB_ID="e2e-copy-job"
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

rm -rf "$ROOT/.e2e"
mkdir -p "$DATA_DIR"
chmod -R a+rwx "$ROOT/.e2e"

docker compose down --remove-orphans >/dev/null 2>&1 || true
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

for _ in $(seq 1 60); do
  if docker compose exec -T postgres pg_isready -U partforge -d partforge >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker compose exec -T postgres pg_isready -U partforge -d partforge >/dev/null

docker compose exec -T clickhouse clickhouse-client --multiquery < e2e/sql/setup_and_freeze.sql

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

for i in $(seq 1 "$part_count"); do
  worker_log="$ROOT/.e2e/worker-${i}.log"
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
    worker \
    -role=inserter \
    -s3-endpoint=http://localstack:4566 \
    -postgres-url="$POSTGRES_URL" \
    -once 2>&1 | tee "$worker_log"
  assert_worker_insert_memory_settings "$worker_log"
done

for i in $(seq 1 6); do
  compact_log="$ROOT/.e2e/compact-${i}.log"
  CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
    worker \
    -role=compactor \
    -s3-endpoint=http://localstack:4566 \
    -postgres-url="$POSTGRES_URL" \
    -compact-window=0s \
    -compact-optimize-final-after=1s \
    -once 2>&1 | tee "$compact_log"

  status="$(
    CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
      job-status \
      -job-id="$JOB_ID" \
      -postgres-url="$POSTGRES_URL" |
      sed -n 's/^status: //p'
  )"
  if [[ "$status" == "READY_FOR_IMPORT" ]]; then
    break
  fi
done

if [[ "${status:-}" != "READY_FOR_IMPORT" ]]; then
  echo "job did not reach READY_FOR_IMPORT; status=${status:-<empty>}" >&2
  exit 1
fi
if ! grep -h "claimed compact-ready batch" "$ROOT"/.e2e/compact-*.log >/dev/null; then
  echo "expected compact worker to claim a compact-ready batch" >&2
  exit 1
fi
if ! grep -Eh "completed compact batch|compact batch did not reduce active part count" "$ROOT"/.e2e/compact-*.log >/dev/null; then
  echo "expected compact worker to finish a compact attempt" >&2
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

CLICKHOUSE_DATA_DIR="$DATA_DIR" docker compose run --rm worker \
  delete-job \
  -job-id="$JOB_ID" \
  -delete-s3 \
  -s3-endpoint=http://localstack:4566 \
  -postgres-url="$POSTGRES_URL"

echo "e2e passed with $part_count frozen parts"
