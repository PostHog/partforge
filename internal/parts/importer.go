package parts

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	artifactpkg "github.com/PostHog/partforge/internal/artifact"
	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/fileutil"
	"github.com/PostHog/partforge/internal/s3copy"
)

type Importer struct {
	S3Copy     s3copy.Copier
	ClickHouse chhttp.Client
	WorkDir    string
}

type FinishedArtifact struct {
	EmptyOutput bool
	Bucket      string
	Key         string
	PartID      string
}

type ImportJob struct {
	Artifacts    []FinishedArtifact
	JobID        string
	Database     string
	Table        string
	RequireEmpty bool
	// Parallelism is how many artifacts are claimed and downloaded at once.
	// Attaches are serialized regardless.
	Parallelism      int
	MarkImporting    func(context.Context, FinishedArtifact) error
	MarkImported     func(context.Context, FinishedArtifact) error
	ReleaseImport    func(context.Context, FinishedArtifact) error
	MarkImportFailed func(context.Context, FinishedArtifact, error) error
}

func (i Importer) ImportJob(ctx context.Context, job ImportJob) error {
	if len(job.Artifacts) == 0 {
		return fmt.Errorf("no finished artifacts found for job %s", job.JobID)
	}
	if job.Parallelism < 1 {
		return fmt.Errorf("import parallelism must be at least 1, got %d", job.Parallelism)
	}
	startedAt := time.Now()
	artifacts := append([]FinishedArtifact(nil), job.Artifacts...)
	sort.Slice(artifacts, func(i, j int) bool {
		return artifacts[i].Key < artifacts[j].Key
	})
	for _, artifact := range artifacts {
		if artifact.Bucket == "" || artifact.Key == "" || artifact.PartID == "" {
			return fmt.Errorf("finished artifact is missing bucket, key, or part_id")
		}
	}

	slog.Info("import job started", "stage", "start_import", "job_id", job.JobID, "destination_table", chhttp.TableSQL(job.Database, job.Table), "artifacts", len(artifacts), "require_empty", job.RequireEmpty, "parallelism", job.Parallelism)
	slog.Info("locating destination table data path", "stage", "prepare_destination", "job_id", job.JobID, "destination_table", chhttp.TableSQL(job.Database, job.Table))
	dataPath, err := i.tableDataPath(ctx, job.Database, job.Table)
	if err != nil {
		return err
	}
	owner, err := ownerFromPath(dataPath)
	if err != nil {
		return fmt.Errorf("stat destination table owner %s: %w", dataPath, err)
	}
	slog.Info("located destination table data path", "stage", "prepare_destination", "job_id", job.JobID, "data_path", dataPath, "owner_uid", owner.uid, "owner_gid", owner.gid)
	detachedPath := filepath.Join(dataPath, "detached")
	if err := os.MkdirAll(detachedPath, 0o755); err != nil {
		return err
	}
	if err := chownPathIfNeeded(detachedPath, owner); err != nil {
		return fmt.Errorf("set detached directory ownership %s: %w", detachedPath, err)
	}
	if job.RequireEmpty {
		slog.Info("checking destination table is empty", "stage", "prepare_destination", "job_id", job.JobID, "destination_table", chhttp.TableSQL(job.Database, job.Table))
		count, err := i.activePartCount(ctx, job.Database, job.Table)
		if err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("destination table %s already has %d active parts; rerunning import-finished could duplicate data", chhttp.TableSQL(job.Database, job.Table), count)
		}
		slog.Info("destination table is empty", "stage", "prepare_destination", "job_id", job.JobID, "destination_table", chhttp.TableSQL(job.Database, job.Table))
	} else {
		slog.Info("skipping destination empty check", "stage", "prepare_destination", "job_id", job.JobID, "destination_table", chhttp.TableSQL(job.Database, job.Table))
	}

	workBase, err := i.importWorkDir(ctx, dataPath)
	if err != nil {
		return err
	}
	root := filepath.Join(workBase, job.JobID)
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	defer os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	if err := ensureSameFilesystem(root, detachedPath); err != nil {
		return err
	}
	workers := min(job.Parallelism, len(artifacts))
	slog.Info("prepared import work directory", "stage", "prepare_destination", "job_id", job.JobID, "work_dir", root, "detached_path", detachedPath, "parallelism", workers)

	// runCtx stops downloads in sibling workers once one artifact fails. ATTACH
	// runs under ctx so a sibling failure never interrupts a partial attach.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	run := importRun{
		attachMu:     &sync.Mutex{},
		importer:     i,
		job:          job,
		total:        len(artifacts),
		root:         root,
		detachedPath: detachedPath,
		owner:        owner,
	}
	var (
		errMu sync.Mutex
		errs  []error
		wg    sync.WaitGroup
	)
	indexes := make(chan int)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range indexes {
				if err := run.importOne(ctx, runCtx, idx, artifacts[idx]); err != nil {
					errMu.Lock()
					errs = append(errs, err)
					errMu.Unlock()
					cancelRun()
				}
			}
		}()
	}
feed:
	for idx := range artifacts {
		select {
		case indexes <- idx:
		case <-runCtx.Done():
			break feed
		}
	}
	close(indexes)
	wg.Wait()
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	elapsed := time.Since(startedAt)
	slog.Info("import complete", "stage", "complete", "job_id", job.JobID, "artifacts", len(artifacts), "elapsed", elapsed, "artifacts_per_second", countRatePerSecond(len(artifacts), elapsed))
	return nil
}

type importRun struct {
	importer     Importer
	job          ImportJob
	total        int
	root         string
	detachedPath string
	owner        pathOwner
	attachMu     *sync.Mutex
}

// importOne claims, downloads, and attaches one artifact. A nil error with a
// cancelled runCtx means the artifact was released back to FINISHED or never
// claimed.
func (r importRun) importOne(ctx, runCtx context.Context, idx int, artifact FinishedArtifact) error {
	if runCtx.Err() != nil {
		return nil
	}
	job := r.job
	workDir := filepath.Join(r.root, fmt.Sprintf("%06d", idx))
	startedAt := time.Now()
	slog.Info(
		"importing artifact",
		"stage", "import_artifact",
		"job_id", job.JobID,
		"artifact_index", idx+1,
		"artifacts_total", r.total,
		"part_id", artifact.PartID,
		"bucket", artifact.Bucket,
		"key", artifact.Key,
	)
	if job.MarkImporting != nil {
		if err := job.MarkImporting(ctx, artifact); err != nil {
			return err
		}
	}
	extractRoot, partNames, err := r.importer.downloadArtifact(runCtx, job, artifact, workDir, r.owner)
	if err != nil {
		if runCtx.Err() != nil {
			return r.release(artifact, workDir, err)
		}
		return r.markFailed(ctx, artifact, err)
	}
	if err := r.attach(ctx, runCtx, artifact, extractRoot, partNames); err != nil {
		if ctx.Err() != nil || errors.Is(err, errAttachSkipped) {
			return r.release(artifact, workDir, err)
		}
		return r.markFailed(ctx, artifact, err)
	}
	if job.MarkImported != nil {
		if err := job.MarkImported(ctx, artifact); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(workDir); err != nil {
		return fmt.Errorf("remove imported artifact work directory %s: %w", workDir, err)
	}
	slog.Info(
		"imported artifact",
		"stage", "import_artifact",
		"job_id", job.JobID,
		"artifact_index", idx+1,
		"artifacts_total", r.total,
		"part_id", artifact.PartID,
		"key", artifact.Key,
		"elapsed", time.Since(startedAt),
	)
	return nil
}

var errAttachSkipped = errors.New("import stopped before attach")

// attach moves extracted parts into detached/ and attaches them. Attaches are
// serialized across workers; once the lock is held and runCtx is still live,
// the attach runs under ctx so only an external cancellation interrupts it.
func (r importRun) attach(ctx, runCtx context.Context, artifact FinishedArtifact, extractRoot string, partNames []string) error {
	r.attachMu.Lock()
	defer r.attachMu.Unlock()
	if runCtx.Err() != nil {
		return errAttachSkipped
	}
	return r.importer.attachParts(ctx, r.job, artifact, r.detachedPath, extractRoot, partNames)
}

func (r importRun) release(artifact FinishedArtifact, workDir string, cause error) error {
	cleanupErr := os.RemoveAll(workDir)
	var releaseErr error
	if r.job.ReleaseImport != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		releaseErr = r.job.ReleaseImport(releaseCtx, artifact)
		cancel()
	}
	if cleanupErr != nil || releaseErr != nil {
		return fmt.Errorf("cancel import artifact s3://%s/%s: %w", artifact.Bucket, artifact.Key, errors.Join(cause, cleanupErr, releaseErr))
	}
	slog.Info("released import artifact", "stage", "import_artifact", "job_id", r.job.JobID, "part_id", artifact.PartID, "key", artifact.Key, "cause", cause)
	return nil
}

func (r importRun) markFailed(ctx context.Context, artifact FinishedArtifact, cause error) error {
	if r.job.MarkImportFailed != nil {
		if markErr := r.job.MarkImportFailed(ctx, artifact, cause); markErr != nil {
			return fmt.Errorf("import artifact s3://%s/%s: %w; additionally failed to mark import failed: %v", artifact.Bucket, artifact.Key, cause, markErr)
		}
	}
	return cause
}

// downloadArtifact downloads and extracts one artifact into workDir and sets
// the destination owner on the extracted parts. Empty outputs return no parts.
func (i Importer) downloadArtifact(ctx context.Context, job ImportJob, artifact FinishedArtifact, workDir string, owner pathOwner) (string, []string, error) {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return "", nil, err
	}
	if artifact.EmptyOutput {
		return "", nil, nil
	}
	downloadRoot := filepath.Join(workDir, "data")
	extractRoot := filepath.Join(workDir, "extracted")
	sourceKey := artifact.Key
	slog.Info("downloading finished artifact", "stage", "download_finished", "job_id", job.JobID, "part_id", artifact.PartID, "bucket", artifact.Bucket, "key", sourceKey)
	downloadStartedAt := time.Now()
	if err := i.S3Copy.DownloadPrefix(ctx, artifact.Bucket, sourceKey, downloadRoot); err != nil {
		return "", nil, fmt.Errorf("download finished artifact s3://%s/%s: %w", artifact.Bucket, sourceKey, err)
	}
	downloadStats, err := fileutil.StatDir(downloadRoot)
	if err != nil {
		return "", nil, fmt.Errorf("stat finished artifact s3://%s/%s: %w", artifact.Bucket, artifact.Key, err)
	}
	downloadElapsed := time.Since(downloadStartedAt)
	slog.Info("downloaded finished artifact", "stage", "download_finished", "job_id", job.JobID, "part_id", artifact.PartID, "files", downloadStats.Files, "bytes", downloadStats.Bytes, "elapsed", downloadElapsed, "bytes_per_second", ratePerSecond(downloadStats.Bytes, downloadElapsed))
	partNames, err := artifactpkg.ExtractFinishedTarballsFromDirContext(ctx, downloadRoot, extractRoot)
	if err != nil {
		return "", nil, fmt.Errorf("extract downloaded finished parts s3://%s/%s: %w", artifact.Bucket, sourceKey, err)
	}
	if len(partNames) == 0 {
		return "", nil, fmt.Errorf("finished artifact s3://%s/%s contains no part tarballs", artifact.Bucket, sourceKey)
	}
	for _, partName := range partNames {
		if err := chownTree(filepath.Join(extractRoot, partName), owner); err != nil {
			return "", nil, fmt.Errorf("set ownership on finished part %s to %d:%d: %w", partName, owner.uid, owner.gid, err)
		}
	}
	return extractRoot, partNames, nil
}

func (i Importer) attachParts(ctx context.Context, job ImportJob, artifact FinishedArtifact, detachedPath, extractRoot string, partNames []string) error {
	for _, partName := range partNames {
		src := filepath.Join(extractRoot, partName)
		dst := filepath.Join(detachedPath, partName)
		if _, err := os.Stat(dst); err == nil {
			return fmt.Errorf("detached part destination already exists: %s", dst)
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := fileutil.MoveDir(src, dst); err != nil {
			return fmt.Errorf("move finished part %s into detached directory: %w", partName, err)
		}
		partStats, err := fileutil.StatDir(dst)
		if err != nil {
			return fmt.Errorf("stat finished part %s: %w", partName, err)
		}
		slog.Info("attaching finished part", "stage", "attach_finished_part", "job_id", job.JobID, "part_id", artifact.PartID, "part", partName, "files", partStats.Files, "bytes", partStats.Bytes)
		if err := i.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(job.Database, job.Table)+" ATTACH PART "+chhttp.StringLiteral(partName)); err != nil {
			return fmt.Errorf("attach finished part %s: %w", partName, err)
		}
	}
	if len(partNames) > 0 {
		slog.Info("attached finished artifact parts", "stage", "attach_finished_part", "job_id", job.JobID, "part_id", artifact.PartID, "key", artifact.Key, "parts", len(partNames))
	}
	return nil
}

func (i Importer) tableDataPath(ctx context.Context, database, table string) (string, error) {
	query := "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database = " +
		chhttp.StringLiteral(database) + " AND name = " + chhttp.StringLiteral(table) + " FORMAT TSV"
	out, err := i.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return "", err
	}
	path := strings.TrimSpace(out)
	if path == "" {
		return "", fmt.Errorf("could not find data path for %s", chhttp.TableSQL(database, table))
	}
	return path, nil
}

type pathOwner struct {
	uid int
	gid int
}

func ownerFromPath(path string) (pathOwner, error) {
	info, err := os.Stat(path)
	if err != nil {
		return pathOwner{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return pathOwner{}, fmt.Errorf("stat %s did not return syscall.Stat_t", path)
	}
	return pathOwner{uid: int(stat.Uid), gid: int(stat.Gid)}, nil
}

func chownTree(root string, owner pathOwner) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		return chownPathIfNeeded(path, owner)
	})
}

func chownPathIfNeeded(path string, owner pathOwner) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("stat %s did not return syscall.Stat_t", path)
	}
	if int(stat.Uid) == owner.uid && int(stat.Gid) == owner.gid {
		return nil
	}
	if err := os.Lchown(path, owner.uid, owner.gid); err != nil {
		return fmt.Errorf("chown %s: %w", path, err)
	}
	return nil
}

func (i Importer) activePartCount(ctx context.Context, database, table string) (uint64, error) {
	query := "SELECT count() FROM system.parts WHERE database = " +
		chhttp.StringLiteral(database) + " AND table = " + chhttp.StringLiteral(table) +
		" AND active FORMAT TSV"
	out, err := i.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return 0, err
	}
	return chhttp.ParseUInt(out)
}

func (i Importer) importWorkDir(ctx context.Context, dataPath string) (string, error) {
	if strings.TrimSpace(i.WorkDir) != "" {
		return filepath.Abs(strings.TrimSpace(i.WorkDir))
	}
	diskPath, err := i.diskPathForDataPath(ctx, dataPath)
	if err != nil {
		return "", err
	}
	return defaultImportWorkDir(diskPath), nil
}

func (i Importer) diskPathForDataPath(ctx context.Context, dataPath string) (string, error) {
	query := "SELECT path, type FROM system.disks ORDER BY length(path) DESC FORMAT TSV"
	out, err := i.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return "", fmt.Errorf("query ClickHouse disks: %w", err)
	}
	rows, err := chhttp.FormatTSVStrings(out, 2)
	if err != nil {
		return "", err
	}
	for _, row := range rows {
		diskPath := strings.TrimSpace(row[0])
		diskType := strings.ToLower(strings.TrimSpace(row[1]))
		if !pathContains(diskPath, dataPath) {
			continue
		}
		if diskType != "local" {
			return "", fmt.Errorf("destination table data path %s is on unsupported ClickHouse disk type %q", dataPath, row[1])
		}
		return diskPath, nil
	}
	return "", fmt.Errorf("could not match destination data path %s to a local ClickHouse disk", dataPath)
}

func defaultImportWorkDir(diskPath string) string {
	return filepath.Join(diskPath, "partforge-import-work")
}

func pathContains(root, child string) bool {
	root = filepath.Clean(root)
	child = filepath.Clean(child)
	rel, err := filepath.Rel(root, child)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func ensureSameFilesystem(a, b string) error {
	aDev, err := deviceID(a)
	if err != nil {
		return err
	}
	bDev, err := deviceID(b)
	if err != nil {
		return err
	}
	if aDev != bDev {
		return fmt.Errorf("import work directory %s and ClickHouse detached directory %s are on different filesystems; set -work-dir to a path on the ClickHouse data disk", a, b)
	}
	return nil
}

func deviceID(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("stat %s did not return syscall.Stat_t", path)
	}
	return uint64(stat.Dev), nil
}

func ratePerSecond(bytes uint64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(bytes) / elapsed.Seconds()
}

func countRatePerSecond(count int, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}
