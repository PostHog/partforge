# Development

## Checks

CI (`.github/workflows/release.yml`) enforces formatting, tidy modules, vet, and tests. Run what CI runs before finishing a change:

```sh
gofmt -l .            # must print nothing
go mod tidy           # must not change go.mod / go.sum
go vet ./...
go test ./...
./e2e/run.sh          # requires Docker
```

`AGENTS.md` lists `go mod tidy && go test ./... && ./e2e/run.sh` as the required pre-commit check.

The e2e script stands up LocalStack, Postgres, and a ClickHouse container, builds the worker image, and runs the full pipeline against `e2e/sql/`, diffing the result against `e2e/expected.tsv`. It builds the image each run; set `PARTFORGE_E2E_SKIP_BUILD=1` to reuse an existing `partforge-worker:latest`.

PostgreSQL integration checks exercise fresh and hand-created schemas, migration rollback and concurrent migration runs, concurrent work claims, ownership, maintenance, and 20,000-row query plans. With local compose Postgres running:

```sh
PARTFORGE_TEST_POSTGRES_URL='postgres://partforge:partforge@localhost:15432/partforge?sslmode=disable' \
  go test ./internal/state -run Postgres -count=1
```

These tests create and remove isolated schemas. They skip when the connection variable is unset. The e2e script also runs `migrate` twice before uploading, checking both initial setup and a no-op rerun.

## Build

```sh
go build -o partforge ./cmd/partforge          # local CLI
docker compose build worker                     # worker image
```

## Project layout

```
cmd/partforge/     CLI entrypoint — every subcommand, flag parsing, config resolution
internal/
  chbackup/        stream and validate native ClickHouse backup indexes
  freeze/          discover ClickHouse disks; scan shadow/<freeze> for frozen parts
  manifest/        per-part manifest.json; job/part ID derivation
  artifact/        write manifests; build/extract part tarballs
  s3copy/          s5cmd wrapper for directory/glob transfers
  state/           Postgres state store — claims, transitions, compaction batches, admin ops
  chproc/          start/stop the local clickhouse-server child process
  chhttp/          ClickHouse HTTP client
  ddl/             CREATE TABLE normalization (Replicated* -> plain MergeTree)
  rewrite/         the worker: per-part processor + compactor
  resources/       CPU/memory detection -> ClickHouse insert & merge tuning
  parts/           import-finished: attach part tarballs into the destination table
  metrics/         Prometheus recorder + metrics HTTP server
  fileutil/        filesystem copy and directory stats
```

Where to change things:

- rewrite / merge-wait / compaction logic → `internal/rewrite`
- state transitions and admin operations → `internal/state`
- ClickHouse tuning heuristics → `internal/resources`
- CLI flags and command wiring → `cmd/partforge/main.go`

## Release

On push to `main`, CI builds and publishes the multi-arch worker image to `ghcr.io/<owner>/partforge` (tagged with the short commit SHA and `latest`) and attaches static `linux/amd64` and `linux/arm64` CLI binaries to a GitHub release named after the SHA. See [deployment.md](deployment.md).
