# Deployment

The worker image is published on every push to `main` to `ghcr.io/<owner>/partforge`, tagged with the short commit SHA and `latest`. CI also attaches static `linux/amd64` and `linux/arm64` CLI binaries to a GitHub release named after the SHA.

The image is a single Ubuntu container with `clickhouse-server`, `clickhouse-client`, `s5cmd`, and the Go binary. Its entrypoint is the binary and the default command is `worker`. It **runs as root** (so it can write its work directory on root-owned host mounts) and starts a local `clickhouse server` child process for each claimed part.

## Recommended: workers on ECS with an IAM task role

Run the workers as an ECS service and give the task an **IAM role** scoped to the S3 bucket and the RDS/Aurora PostgreSQL database user. This is the recommended setup:

- **No static credentials.** The AWS SDK picks up temporary credentials from the ECS task role via the container credentials endpoint. Do not bake access keys into the image or config.
- **Region resolves from the environment.** Set `AWS_REGION` or pass `-aws-region` when `-postgres-iam-auth` is enabled.
- **Scale by replicas.** More worker tasks = more parts in flight. There is no coordinator to scale; workers claim independently from Postgres.

### ECS task scale-in protection

Workers automatically use the ECS agent's task scale-in protection endpoint when `ECS_AGENT_URI` is available. A worker protects its task after claiming rewrite or compaction work and removes protection after committing the result. Outside ECS, or when the agent does not expose the endpoint, worker behavior is unchanged.

The task role needs `ecs:GetTaskProtection` and `ecs:UpdateTaskProtection`. If the endpoint is detected but protection cannot be changed, the worker releases newly claimed work and exits instead of processing it unprotected.

### Task IAM policy

Combine the S3 permissions with `rds-db:connect` for the Postgres database user. Database setup details are in [postgres.md](postgres.md).

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "rds-db:connect",
      "Resource": "arn:aws:rds-db:us-east-1:123456789012:dbuser:db-ABCDEFGHIJKLMNOP/partforge"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::partforge"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::partforge/*"
    }
  ]
}
```

`s3:DeleteObject` is needed because workers replace finished-artifact prefixes (`s5cmd rm` then upload) and admin `-delete-s3` operations remove artifacts.

### Storage matters

Worker scratch (`-work-dir`) holds the local ClickHouse data plus downloaded source parts, and compaction transiently holds downloaded tarballs, extracted parts, merge output, and re-uploaded tarballs at once. It must be **fast local disk with enough headroom**:

- **EC2 launch type with instance-store NVMe** is best for large parts — mount the NVMe into the container and set `-work-dir` on it (e.g. `/mnt/nvme/partforge-work`).
- **Fargate** works for smaller jobs; size task ephemeral storage for the largest rewritten artifact.

Each claimed part gets its own `run-*` directory that is removed when the part finishes.

### Splitting inserter and compactor

Run the rewrite and compaction stages as separate services to scale them independently:

- `worker -role=inserter` — rewrite only.
- `worker -role=compactor` — compaction only.
- `worker -role=all` (default) — rewrite first, compact when idle.

See [operations.md](operations.md) for the full flag set and metrics.

## Where the other commands run

`worker` is the only stage that belongs on ECS. The other two need local access to a ClickHouse node's disks and generally run there:

- **`upload-freeze`** must run where it can read the source ClickHouse data disks reported by `system.disks`.
- **`import-finished`** must run where its work-dir shares a filesystem with the destination table's `detached` directory (parts are moved, not copied).

Both still need the same S3 and Postgres access as the workers.
