package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	rdsauth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusReady        Status = "READY"
	StatusInProgress   Status = "IN_PROGRESS"
	StatusCompactReady Status = "COMPACT_READY"
	StatusCompacting   Status = "COMPACTING"
	StatusSuperseded   Status = "SUPERSEDED"
	StatusFinished     Status = "FINISHED"
	StatusImporting    Status = "IMPORTING"
	StatusImported     Status = "IMPORTED"
	StatusFailed       Status = "FAILED"

	MaxCompactBatchParts = 99

	timeFormat        = "2006-01-02T15:04:05.000000000Z"
	defaultRegion     = "us-east-1"
	defaultStateTable = "partforge_state"
)

type Status string

var allStatuses = []Status{
	StatusReady,
	StatusInProgress,
	StatusCompactReady,
	StatusCompacting,
	StatusSuperseded,
	StatusFinished,
	StatusImporting,
	StatusImported,
	StatusFailed,
}

type Config struct {
	Region   string
	Endpoint string
	Table    string
	IAMAuth  bool
}

type Store struct {
	pool              *pgxpool.Pool
	tableSQL          string
	statusIndexSQL    string
	jobStatusIndexSQL string
}

type Part struct {
	JobID          string `json:"job_id"`
	JobName        string `json:"job_name,omitempty"`
	PartID         string `json:"part_id"`
	Status         Status `json:"status"`
	Bucket         string `json:"bucket"`
	SourceKey      string `json:"source_key"`
	FinishedKey    string `json:"finished_key"`
	SourceJobID    string `json:"source_job_id,omitempty"`
	SourcePartID   string `json:"source_part_id,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
	StartedAt      string `json:"started_at,omitempty"`
	FinishedAt     string `json:"finished_at,omitempty"`
	CompactReadyAt string `json:"compact_ready_at,omitempty"`
	CompactingAt   string `json:"compacting_at,omitempty"`
	SupersededAt   string `json:"superseded_at,omitempty"`
	ImportingAt    string `json:"importing_at,omitempty"`
	ImportedAt     string `json:"imported_at,omitempty"`
	FailedAt       string `json:"failed_at,omitempty"`
	WorkerID       string `json:"worker_id,omitempty"`
	Attempts       int    `json:"attempts"`
	Error          string `json:"error,omitempty"`

	DestinationDatabase  string   `json:"destination_database,omitempty"`
	DestinationTable     string   `json:"destination_table,omitempty"`
	DestinationSchema    string   `json:"destination_schema,omitempty"`
	InsertSelect         string   `json:"insert_select,omitempty"`
	CompactGeneration    int      `json:"compact_generation,omitempty"`
	CompactInputPartIDs  []string `json:"compact_input_part_ids,omitempty"`
	CompactCooldownUntil string   `json:"compact_cooldown_until,omitempty"`
	SupersededBy         string   `json:"superseded_by,omitempty"`

	CompactOutputPartID        string  `json:"compact_output_part_id,omitempty"`
	CompactProgressAt          string  `json:"compact_progress_at,omitempty"`
	CompactFinalizeRequestedAt string  `json:"compact_finalize_requested_at,omitempty"`
	CompactInputPartCount      uint64  `json:"compact_input_part_count,omitempty"`
	CompactInputRows           uint64  `json:"compact_input_rows,omitempty"`
	CompactInputBytes          uint64  `json:"compact_input_bytes,omitempty"`
	CompactOutputPartCount     uint64  `json:"compact_output_part_count,omitempty"`
	CompactOutputRows          uint64  `json:"compact_output_rows,omitempty"`
	CompactOutputBytes         uint64  `json:"compact_output_bytes,omitempty"`
	CompactStage               string  `json:"compact_stage,omitempty"`
	CompactActiveMerges        uint64  `json:"compact_active_merges,omitempty"`
	CompactMergeProgress       float64 `json:"compact_merge_progress,omitempty"`

	ProgressUpdatedAt                string            `json:"progress_updated_at,omitempty"`
	ReadRows                         uint64            `json:"read_rows,omitempty"`
	ReadBytes                        uint64            `json:"read_bytes,omitempty"`
	WrittenRows                      uint64            `json:"written_rows,omitempty"`
	WrittenBytes                     uint64            `json:"written_bytes,omitempty"`
	SourceActivePartCount            uint64            `json:"source_active_part_count,omitempty"`
	SourceActivePartRows             uint64            `json:"source_active_part_rows,omitempty"`
	SourceActivePartBytes            uint64            `json:"source_active_part_bytes,omitempty"`
	DestinationActivePartCount       uint64            `json:"destination_active_part_count,omitempty"`
	DestinationActivePartRows        uint64            `json:"destination_active_part_rows,omitempty"`
	DestinationActivePartBytes       uint64            `json:"destination_active_part_bytes,omitempty"`
	DestinationActivePartitionCounts map[string]uint64 `json:"destination_active_partition_counts,omitempty"`
	DestinationFailedMerges          uint64            `json:"destination_failed_merges,omitempty"`
	RewriteStage                     string            `json:"rewrite_stage,omitempty"`
	RewriteStageStartedAt            string            `json:"rewrite_stage_started_at,omitempty"`
	RewriteStageElapsedMs            int64             `json:"rewrite_stage_elapsed_ms,omitempty"`
	RewriteTotalElapsedMs            int64             `json:"rewrite_total_elapsed_ms,omitempty"`
	RewriteStageDurationsMs          map[string]int64  `json:"rewrite_stage_durations_ms,omitempty"`
}

type Job struct {
	JobID                      string         `json:"job_id"`
	Name                       string         `json:"name,omitempty"`
	Total                      int            `json:"total"`
	Counts                     map[Status]int `json:"counts,omitempty"`
	DestinationActivePartCount uint64         `json:"destination_active_part_count,omitempty"`
	DestinationPartitionCount  int            `json:"destination_partition_count,omitempty"`
	SubmittedAt                string         `json:"submitted_at,omitempty"`
	UpdatedAt                  string         `json:"updated_at,omitempty"`
}

type QueryProgress struct {
	ReadRows     uint64
	ReadBytes    uint64
	WrittenRows  uint64
	WrittenBytes uint64
}

type PartStats struct {
	Count uint64
	Rows  uint64
	Bytes uint64
}

func clonePartitionCounts(counts map[string]uint64) map[string]uint64 {
	if len(counts) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(counts))
	for partitionID, count := range counts {
		if strings.TrimSpace(partitionID) == "" || count == 0 {
			continue
		}
		out[partitionID] = count
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

type CompactClaimOptions struct {
	MaxArtifacts         int
	MaxBytes             uint64
	MinInputParts        uint64
	ExcludedJobIDs       map[string]struct{}
	JobID                string
	Bucket               string
	DestinationDatabase  string
	DestinationTable     string
	DestinationSchema    string
	RequiredPartitionIDs []string
}

type CompactBatch struct {
	JobID          string
	Parts          []Part
	InputPartCount uint64
	InputRows      uint64
	InputBytes     uint64
	Generation     int
}

type RewriteProgress struct {
	QueryProgress              *QueryProgress
	SourceActivePartStats      *PartStats
	DestinationActivePartStats *PartStats
	DestinationFailedMerges    *uint64
	StageProgress              *RewriteStageProgress
}

type RewriteStageProgress struct {
	Stage                     string
	StageStartedAt            time.Time
	StageElapsedMs            int64
	TotalElapsedMs            int64
	CompletedStageDurationsMs map[string]int64
}

func New(ctx context.Context, cfg Config) (*Store, error) {
	if strings.TrimSpace(cfg.Table) == "" {
		cfg.Table = defaultStateTable
	}
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("postgres state URL is required")
	}
	tableSQL, err := quoteTableName(cfg.Table)
	if err != nil {
		return nil, err
	}
	statusIndexSQL := quoteIndexName(cfg.Table, "status_idx")
	jobStatusIndexSQL := quoteIndexName(cfg.Table, "job_status_idx")
	poolCfg, err := pgxpool.ParseConfig(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse postgres state URL: %w", err)
	}
	if cfg.IAMAuth {
		if err := configureIAMAuth(ctx, poolCfg, cfg.Region); err != nil {
			return nil, err
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("open postgres state store: %w", err)
	}
	store := &Store{
		pool:              pool,
		tableSQL:          tableSQL,
		statusIndexSQL:    statusIndexSQL,
		jobStatusIndexSQL: jobStatusIndexSQL,
	}
	if err := store.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

func configureIAMAuth(ctx context.Context, poolCfg *pgxpool.Config, region string) error {
	loadOptions := []func(*config.LoadOptions) error{}
	if strings.TrimSpace(region) != "" {
		loadOptions = append(loadOptions, config.WithRegion(strings.TrimSpace(region)))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return fmt.Errorf("load AWS config for postgres IAM auth: %w", err)
	}
	if strings.TrimSpace(awsCfg.Region) == "" {
		awsCfg.Region = defaultRegion
	}
	base := poolCfg.BeforeConnect
	poolCfg.BeforeConnect = func(ctx context.Context, connCfg *pgx.ConnConfig) error {
		if base != nil {
			if err := base(ctx, connCfg); err != nil {
				return err
			}
		}
		if strings.TrimSpace(connCfg.User) == "" {
			return errors.New("postgres user is required for IAM auth")
		}
		if strings.TrimSpace(connCfg.Host) == "" || connCfg.Port == 0 {
			return errors.New("postgres host and port are required for IAM auth")
		}
		endpoint := net.JoinHostPort(connCfg.Host, strconv.Itoa(int(connCfg.Port)))
		token, err := rdsauth.BuildAuthToken(ctx, endpoint, awsCfg.Region, connCfg.User, awsCfg.Credentials)
		if err != nil {
			return fmt.Errorf("build postgres IAM auth token: %w", err)
		}
		connCfg.Password = token
		return nil
	}
	return nil
}

func quoteTableName(name string) (string, error) {
	parts := strings.Split(strings.TrimSpace(name), ".")
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return "", fmt.Errorf("invalid postgres state table %q", name)
		}
	}
	return pgx.Identifier(parts).Sanitize(), nil
}

func quoteIndexName(table, suffix string) string {
	base := strings.NewReplacer(".", "_", "-", "_").Replace(strings.TrimSpace(table))
	base = strings.Trim(base, "_")
	if base == "" {
		base = defaultStateTable
	}
	maxBaseLen := 63 - len(suffix) - 1
	if maxBaseLen < 1 {
		maxBaseLen = 1
	}
	if len(base) > maxBaseLen {
		base = base[:maxBaseLen]
	}
	return pgx.Identifier{base + "_" + suffix}.Sanitize()
}

func (s *Store) ensureSchema(ctx context.Context) error {
	statements := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			job_id text NOT NULL,
			part_id text NOT NULL,
			status text NOT NULL,
			worker_id text NOT NULL DEFAULT '',
			created_at text NOT NULL,
			updated_at text NOT NULL,
			data jsonb NOT NULL,
			PRIMARY KEY (job_id, part_id)
		)`, s.tableSQL),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (status, created_at, job_id, part_id)`, s.statusIndexSQL, s.tableSQL),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (job_id, status, part_id)`, s.jobStatusIndexSQL, s.tableSQL),
	}
	for _, statement := range statements {
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("ensure postgres state schema: %w", err)
		}
	}
	return nil
}

func NewPart(jobID, partID, bucket, sourceKey, finishedKey string, now time.Time) Part {
	createdAt := formatTime(now)
	return Part{
		JobID:       jobID,
		PartID:      partID,
		Status:      StatusReady,
		Bucket:      bucket,
		SourceKey:   sourceKey,
		FinishedKey: finishedKey,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}
}

func NewCompactPart(jobID, partID, bucket, finishedKey, database, table, destinationSchema string, inputPartIDs []string, generation int, stats PartStats, partitionCounts map[string]uint64, compactReadyAt time.Time, now time.Time) Part {
	createdAt := formatTime(now)
	return Part{
		JobID:                            jobID,
		PartID:                           partID,
		Status:                           StatusCompactReady,
		Bucket:                           bucket,
		SourceKey:                        finishedKey,
		FinishedKey:                      finishedKey,
		CreatedAt:                        createdAt,
		UpdatedAt:                        createdAt,
		CompactReadyAt:                   formatTime(compactReadyAt),
		DestinationDatabase:              database,
		DestinationTable:                 table,
		DestinationSchema:                destinationSchema,
		CompactGeneration:                generation,
		CompactInputPartIDs:              append([]string(nil), inputPartIDs...),
		DestinationActivePartCount:       stats.Count,
		DestinationActivePartRows:        stats.Rows,
		DestinationActivePartBytes:       stats.Bytes,
		DestinationActivePartitionCounts: clonePartitionCounts(partitionCounts),
	}
}

type conditionalCheckFailedError struct {
	message string
}

func (e *conditionalCheckFailedError) Error() string {
	if e.message == "" {
		return "conditional check failed"
	}
	return e.message
}

func partJSON(part Part) ([]byte, error) {
	if err := validatePart(part); err != nil {
		return nil, err
	}
	return json.Marshal(part)
}

func partFromJSON(data []byte) (Part, error) {
	var part Part
	if err := json.Unmarshal(data, &part); err != nil {
		return Part{}, err
	}
	if err := validatePart(part); err != nil {
		return Part{}, err
	}
	return part, nil
}

func (s *Store) savePartTx(ctx context.Context, tx pgx.Tx, part Part) error {
	data, err := partJSON(part)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE `+s.tableSQL+` SET status = $1, worker_id = $2, created_at = $3, updated_at = $4, data = $5 WHERE job_id = $6 AND part_id = $7`,
		string(part.Status), part.WorkerID, part.CreatedAt, part.UpdatedAt, data, part.JobID, part.PartID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return &conditionalCheckFailedError{message: fmt.Sprintf("part %s/%s was not updated", part.JobID, part.PartID)}
	}
	return nil
}

func (s *Store) readPartTx(ctx context.Context, tx pgx.Tx, jobID, partID string) (Part, error) {
	var data []byte
	err := tx.QueryRow(ctx, `SELECT data FROM `+s.tableSQL+` WHERE job_id = $1 AND part_id = $2 FOR UPDATE`, jobID, partID).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Part{}, &conditionalCheckFailedError{message: fmt.Sprintf("part %s/%s does not exist", jobID, partID)}
	}
	if err != nil {
		return Part{}, err
	}
	return partFromJSON(data)
}

func (s *Store) updatePart(ctx context.Context, jobID, partID string, condition func(Part) bool, mutate func(*Part) error) (Part, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Part{}, err
	}
	defer tx.Rollback(ctx)

	part, err := s.readPartTx(ctx, tx, jobID, partID)
	if err != nil {
		return Part{}, err
	}
	if condition != nil && !condition(part) {
		return Part{}, &conditionalCheckFailedError{message: fmt.Sprintf("part %s/%s did not match expected state", jobID, partID)}
	}
	if mutate != nil {
		if err := mutate(&part); err != nil {
			return Part{}, err
		}
	}
	if err := s.savePartTx(ctx, tx, part); err != nil {
		return Part{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Part{}, err
	}
	return part, nil
}

func setStatus(part *Part, status Status, now time.Time) {
	part.Status = status
	part.UpdatedAt = formatTime(now)
}

func clearRewriteProgress(part *Part) {
	part.ProgressUpdatedAt = ""
	part.ReadRows = 0
	part.ReadBytes = 0
	part.WrittenRows = 0
	part.WrittenBytes = 0
	part.SourceActivePartCount = 0
	part.SourceActivePartRows = 0
	part.SourceActivePartBytes = 0
	part.DestinationActivePartCount = 0
	part.DestinationActivePartRows = 0
	part.DestinationActivePartBytes = 0
	part.DestinationActivePartitionCounts = nil
	part.DestinationFailedMerges = 0
	part.RewriteStage = ""
	part.RewriteStageStartedAt = ""
	part.RewriteStageElapsedMs = 0
	part.RewriteTotalElapsedMs = 0
	part.RewriteStageDurationsMs = nil
}

func clearCompactProgress(part *Part) {
	part.CompactOutputPartID = ""
	part.CompactProgressAt = ""
	part.CompactFinalizeRequestedAt = ""
	part.CompactInputPartCount = 0
	part.CompactInputRows = 0
	part.CompactInputBytes = 0
	part.CompactOutputPartCount = 0
	part.CompactOutputRows = 0
	part.CompactOutputBytes = 0
	part.CompactStage = ""
	part.CompactActiveMerges = 0
	part.CompactMergeProgress = 0
}

func compactOwnedOrUnownedReady(part Part, workerID string) bool {
	return (part.Status == StatusCompacting && part.WorkerID == workerID) ||
		(part.Status == StatusCompactReady && strings.TrimSpace(part.WorkerID) == "")
}

func (s *Store) CreatePart(ctx context.Context, part Part) error {
	if err := validatePart(part); err != nil {
		return err
	}
	data, err := partJSON(part)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if strings.TrimSpace(part.SourceJobID) != "" {
		source, err := s.readPartTx(ctx, tx, part.SourceJobID, part.SourcePartID)
		if err != nil {
			return fmt.Errorf("validate source part reference for %s/%s: %w", part.JobID, part.PartID, err)
		}
		if isGeneratedCompactPart(source) {
			return fmt.Errorf("source part reference for %s/%s points at generated compact part %s/%s", part.JobID, part.PartID, source.JobID, source.PartID)
		}
		if source.Bucket != part.Bucket || source.SourceKey != part.SourceKey {
			return fmt.Errorf("source part reference for %s/%s does not match source artifact %s/%s", part.JobID, part.PartID, source.JobID, source.PartID)
		}
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO `+s.tableSQL+` (job_id, part_id, status, worker_id, created_at, updated_at, data) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		part.JobID, part.PartID, string(part.Status), part.WorkerID, part.CreatedAt, part.UpdatedAt, data,
	)
	if err != nil {
		return fmt.Errorf("create state item for %s/%s: %w", part.JobID, part.PartID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) MarkCompactReady(ctx context.Context, part Part, workerID, finishedKey, database, table, destinationSchema string, stats PartStats, partitionCounts map[string]uint64, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if strings.TrimSpace(finishedKey) == "" {
		return errors.New("finished key is required")
	}
	if strings.TrimSpace(database) == "" || strings.TrimSpace(table) == "" || strings.TrimSpace(destinationSchema) == "" {
		return errors.New("destination database, table, and schema are required")
	}
	if stats.Count > 0 && len(partitionCounts) == 0 {
		return fmt.Errorf("destination partition counts are required when destination active part count is %d", stats.Count)
	}
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusInProgress && current.WorkerID == workerID
	}, func(current *Part) error {
		setStatus(current, StatusCompactReady, now)
		current.FinishedKey = finishedKey
		current.CompactReadyAt = formatTime(now)
		current.DestinationDatabase = database
		current.DestinationTable = table
		current.DestinationSchema = destinationSchema
		current.CompactGeneration = 0
		current.DestinationActivePartCount = stats.Count
		current.DestinationActivePartRows = stats.Rows
		current.DestinationActivePartBytes = stats.Bytes
		current.DestinationActivePartitionCounts = clonePartitionCounts(partitionCounts)
		current.WorkerID = ""
		current.Error = ""
		current.CompactCooldownUntil = ""
		return nil
	})
	if err != nil {
		return fmt.Errorf("mark part %s/%s compact ready: %w", part.JobID, part.PartID, err)
	}
	return nil
}

func (s *Store) ClaimNextReady(ctx context.Context, workerID string, now time.Time) (*Part, error) {
	if strings.TrimSpace(workerID) == "" {
		return nil, errors.New("worker id is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var data []byte
	err = tx.QueryRow(ctx, `SELECT data FROM `+s.tableSQL+` WHERE status = $1 ORDER BY created_at, job_id, part_id LIMIT 1 FOR UPDATE SKIP LOCKED`, string(StatusReady)).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query ready parts: %w", err)
	}
	part, err := partFromJSON(data)
	if err != nil {
		return nil, err
	}
	claimPartInMemory(&part, workerID, now)
	if err := s.savePartTx(ctx, tx, part); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &part, nil
}

func (s *Store) ClaimNextCompactBatch(ctx context.Context, workerID string, now time.Time, opts CompactClaimOptions) (*CompactBatch, error) {
	if strings.TrimSpace(workerID) == "" {
		return nil, errors.New("worker id is required")
	}
	if opts.MaxArtifacts < 0 {
		return nil, fmt.Errorf("compact max artifacts must be non-negative, got %d", opts.MaxArtifacts)
	}

	candidates, err := s.listPartsByStatusIndex(ctx, StatusCompactReady)
	if err != nil {
		return nil, fmt.Errorf("query compact-ready parts: %w", err)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	compacting, err := s.listPartsByStatusIndex(ctx, StatusCompacting)
	if err != nil {
		return nil, fmt.Errorf("query compacting parts: %w", err)
	}

	groups := compactCandidateGroups(candidates, compacting, opts)
	for _, group := range groups {
		selected := selectCompactBatchParts(group, opts)
		if len(selected) == 0 {
			continue
		}
		claimed, err := s.claimCompactParts(ctx, selected, workerID, now)
		if IsConditionalCheckFailed(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		batch, err := compactBatchFromParts(claimed)
		if err != nil {
			_ = s.ReleaseCompactBatch(ctx, CompactBatch{JobID: claimed[0].JobID, Parts: claimed}, workerID, now)
			return nil, err
		}
		return batch, nil
	}
	return nil, nil
}

func (s *Store) listPartsByStatusIndex(ctx context.Context, status Status) ([]Part, error) {
	rows, err := s.pool.Query(ctx, `SELECT data FROM `+s.tableSQL+` WHERE status = $1 ORDER BY created_at, job_id, part_id`, string(status))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []Part
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		part, err := partFromJSON(data)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return parts, nil
}

func (s *Store) ReleaseCompactBatch(ctx context.Context, batch CompactBatch, workerID string, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	for _, part := range batch.Parts {
		_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
			return compactOwnedOrUnownedReady(current, workerID)
		}, func(current *Part) error {
			setStatus(current, StatusCompactReady, now)
			if strings.TrimSpace(current.CompactReadyAt) == "" {
				current.CompactReadyAt = compactReadyAtForRelease(part, now)
			}
			current.WorkerID = ""
			current.CompactingAt = ""
			current.Error = ""
			current.CompactCooldownUntil = ""
			clearCompactProgress(current)
			return nil
		})
		if err != nil {
			return fmt.Errorf("release compacting part %s/%s: %w", part.JobID, part.PartID, err)
		}
	}
	return nil
}

func (s *Store) MarkCompactBatchFailed(ctx context.Context, batch CompactBatch, workerID string, cause error, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if cause == nil {
		return errors.New("failure cause is required")
	}
	if err := validateCompactBatch(batch); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, part := range batch.Parts {
		current, err := s.readPartTx(ctx, tx, part.JobID, part.PartID)
		if err != nil {
			return fmt.Errorf("mark compact batch %s failed: %w", batch.JobID, err)
		}
		if current.Status != StatusCompacting || current.WorkerID != workerID {
			return fmt.Errorf("mark compact batch %s failed: %w", batch.JobID, &conditionalCheckFailedError{})
		}
		markCompactPartFailed(&current, cause, now)
		if err := s.savePartTx(ctx, tx, current); err != nil {
			return fmt.Errorf("mark compact batch %s failed: %w", batch.JobID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("mark compact batch %s failed: %w", batch.JobID, err)
	}
	return nil
}

func markCompactPartFailed(part *Part, cause error, now time.Time) {
	setStatus(part, StatusFailed, now)
	part.FailedAt = formatTime(now)
	part.WorkerID = ""
	part.CompactingAt = ""
	part.Error = cause.Error()
	part.CompactCooldownUntil = ""
	clearCompactProgress(part)
}

func (s *Store) HeartbeatCompactBatch(ctx context.Context, batch CompactBatch, workerID string, now time.Time) (bool, error) {
	if strings.TrimSpace(workerID) == "" {
		return false, errors.New("worker id is required")
	}
	finalizeRequested := false
	for _, part := range batch.Parts {
		updated, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
			return compactOwnedOrUnownedReady(current, workerID)
		}, func(current *Part) error {
			setStatus(current, StatusCompacting, now)
			if strings.TrimSpace(current.CompactingAt) == "" {
				current.CompactingAt = formatTime(now)
			}
			current.WorkerID = workerID
			current.Error = ""
			current.CompactCooldownUntil = ""
			return nil
		})
		if err != nil {
			return false, fmt.Errorf("heartbeat compacting part %s/%s: %w", part.JobID, part.PartID, err)
		}
		if strings.TrimSpace(updated.CompactFinalizeRequestedAt) != "" {
			finalizeRequested = true
		}
	}
	return finalizeRequested, nil
}

func (s *Store) RequestCompactFinalization(ctx context.Context, part Part, now time.Time) error {
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusCompacting
	}, func(current *Part) error {
		current.CompactFinalizeRequestedAt = formatTime(now)
		current.UpdatedAt = formatTime(now)
		return nil
	})
	if err != nil {
		return fmt.Errorf("request compact finalization for %s/%s: %w", part.JobID, part.PartID, err)
	}
	return nil
}

type CompactProgress struct {
	Stage         string
	ActiveMerges  uint64
	MergeProgress float64
}

func (s *Store) UpdateCompactProgress(ctx context.Context, batch CompactBatch, outputPartID, workerID string, inputStats, outputStats PartStats, progress CompactProgress, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if strings.TrimSpace(outputPartID) == "" {
		return errors.New("compact output part id is required")
	}
	if err := validateCompactBatch(batch); err != nil {
		return err
	}
	if progress.MergeProgress < 0 {
		return fmt.Errorf("compact merge progress must be non-negative, got %f", progress.MergeProgress)
	}
	for _, part := range batch.Parts {
		_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
			return compactOwnedOrUnownedReady(current, workerID)
		}, func(current *Part) error {
			setStatus(current, StatusCompacting, now)
			if strings.TrimSpace(current.CompactingAt) == "" {
				current.CompactingAt = formatTime(now)
			}
			current.WorkerID = workerID
			current.CompactProgressAt = formatTime(now)
			current.CompactOutputPartID = outputPartID
			current.CompactInputPartCount = inputStats.Count
			current.CompactInputRows = inputStats.Rows
			current.CompactInputBytes = inputStats.Bytes
			current.CompactOutputPartCount = outputStats.Count
			current.CompactOutputRows = outputStats.Rows
			current.CompactOutputBytes = outputStats.Bytes
			current.CompactStage = strings.TrimSpace(progress.Stage)
			current.CompactActiveMerges = progress.ActiveMerges
			current.CompactMergeProgress = progress.MergeProgress
			current.Error = ""
			current.CompactCooldownUntil = ""
			return nil
		})
		if err != nil {
			return fmt.Errorf("update compact progress for %s/%s: %w", part.JobID, part.PartID, err)
		}
	}
	return nil
}

func (s *Store) ReleaseStaleCompactingParts(ctx context.Context, now time.Time, staleAfter time.Duration) (int, error) {
	if staleAfter <= 0 {
		return 0, fmt.Errorf("compact stale timeout must be greater than zero, got %s", staleAfter)
	}
	parts, err := s.listPartsByStatusIndex(ctx, StatusCompacting)
	if err != nil {
		return 0, fmt.Errorf("query compacting parts: %w", err)
	}
	cutoff := now.Add(-staleAfter)
	released := 0
	for _, part := range parts {
		staleAt, err := compactStaleTime(part)
		if err != nil {
			return released, err
		}
		if staleAt.After(cutoff) {
			continue
		}
		ok, err := s.releaseStaleCompactingPart(ctx, part, now)
		if err != nil {
			return released, err
		}
		if ok {
			released++
		}
	}
	return released, nil
}

func (s *Store) releaseStaleCompactingPart(ctx context.Context, part Part, now time.Time) (bool, error) {
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusCompacting && current.UpdatedAt == part.UpdatedAt
	}, func(current *Part) error {
		setStatus(current, StatusCompactReady, now)
		if strings.TrimSpace(current.CompactReadyAt) == "" {
			current.CompactReadyAt = compactReadyAtForRelease(part, now)
		}
		current.WorkerID = ""
		current.CompactingAt = ""
		current.Error = ""
		current.CompactCooldownUntil = ""
		clearCompactProgress(current)
		return nil
	})
	if IsConditionalCheckFailed(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("release stale compacting part %s/%s: %w", part.JobID, part.PartID, err)
	}
	return true, nil
}

func (s *Store) CompleteCompaction(ctx context.Context, batch CompactBatch, output Part, workerID string, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if len(batch.Parts) > MaxCompactBatchParts {
		return fmt.Errorf("compact batch has %d input parts, exceeds compact transaction limit", len(batch.Parts))
	}
	if err := validateCompactBatch(batch); err != nil {
		return err
	}
	if err := validatePart(output); err != nil {
		return err
	}
	if output.Status != StatusCompactReady {
		return fmt.Errorf("compact output %s/%s is %s, expected %s", output.JobID, output.PartID, output.Status, StatusCompactReady)
	}
	if err := validateCompactOutputForBatch(batch, output); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	outputData, err := partJSON(output)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO `+s.tableSQL+` (job_id, part_id, status, worker_id, created_at, updated_at, data) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		output.JobID, output.PartID, string(output.Status), output.WorkerID, output.CreatedAt, output.UpdatedAt, outputData,
	); err != nil {
		return fmt.Errorf("complete compaction for %s/%s: %w", batch.JobID, output.PartID, err)
	}
	for _, part := range batch.Parts {
		current, err := s.readPartTx(ctx, tx, part.JobID, part.PartID)
		if err != nil {
			return fmt.Errorf("complete compaction for %s/%s: %w", batch.JobID, output.PartID, err)
		}
		if !compactOwnedOrUnownedReady(current, workerID) {
			return fmt.Errorf("complete compaction for %s/%s: %w", batch.JobID, output.PartID, &conditionalCheckFailedError{})
		}
		setStatus(&current, StatusSuperseded, now)
		current.SupersededAt = formatTime(now)
		current.SupersededBy = output.PartID
		current.WorkerID = ""
		current.CompactingAt = ""
		current.Error = ""
		current.CompactCooldownUntil = ""
		clearCompactProgress(&current)
		if err := s.savePartTx(ctx, tx, current); err != nil {
			return fmt.Errorf("complete compaction for %s/%s: %w", batch.JobID, output.PartID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("complete compaction for %s/%s: %w", batch.JobID, output.PartID, err)
	}
	return nil
}

func (s *Store) MarkCompactReadyFinished(ctx context.Context, part Part, now time.Time) error {
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusCompactReady
	}, func(current *Part) error {
		setStatus(current, StatusFinished, now)
		current.FinishedAt = formatTime(now)
		current.Error = ""
		current.CompactCooldownUntil = ""
		return nil
	})
	if err != nil {
		return fmt.Errorf("mark compact-ready part %s/%s finished: %w", part.JobID, part.PartID, err)
	}
	return nil
}

type compactGroup struct {
	key                    string
	parts                  []Part
	compactingPartitionIDs []string
}

func compactCandidateGroups(parts, compacting []Part, opts CompactClaimOptions) []compactGroup {
	groupsByKey := map[string][]Part{}
	var order []string
	for _, part := range parts {
		if strings.TrimSpace(part.DestinationDatabase) == "" ||
			strings.TrimSpace(part.DestinationTable) == "" ||
			strings.TrimSpace(part.DestinationSchema) == "" ||
			part.DestinationActivePartCount == 0 ||
			len(part.DestinationActivePartitionCounts) == 0 ||
			!matchesCompactClaimOptions(part, opts) {
			continue
		}
		key := compactGroupKey(part)
		if _, ok := groupsByKey[key]; !ok {
			order = append(order, key)
		}
		groupsByKey[key] = append(groupsByKey[key], part)
	}
	compactingPartitionsByKey := compactingPartitionIDsByGroup(compacting)
	groups := make([]compactGroup, 0, len(order))
	for _, key := range order {
		groupParts := groupsByKey[key]
		sort.SliceStable(groupParts, func(i, j int) bool {
			if groupParts[i].CompactGeneration != groupParts[j].CompactGeneration {
				return groupParts[i].CompactGeneration < groupParts[j].CompactGeneration
			}
			if groupParts[i].UpdatedAt != groupParts[j].UpdatedAt {
				return groupParts[i].UpdatedAt < groupParts[j].UpdatedAt
			}
			return groupParts[i].PartID < groupParts[j].PartID
		})
		groups = append(groups, compactGroup{
			key:                    key,
			parts:                  groupParts,
			compactingPartitionIDs: compactingPartitionsByKey[key],
		})
	}
	return groups
}

func compactGroupKey(part Part) string {
	return strings.Join([]string{part.JobID, part.Bucket, part.DestinationDatabase, part.DestinationTable, part.DestinationSchema}, "\x00")
}

func compactingPartitionIDsByGroup(parts []Part) map[string][]string {
	sets := map[string]map[string]struct{}{}
	for _, part := range parts {
		if part.Status != StatusCompacting ||
			strings.TrimSpace(part.DestinationDatabase) == "" ||
			strings.TrimSpace(part.DestinationTable) == "" ||
			strings.TrimSpace(part.DestinationSchema) == "" {
			continue
		}
		key := compactGroupKey(part)
		if _, ok := sets[key]; !ok {
			sets[key] = map[string]struct{}{}
		}
		for _, partitionID := range partPartitionIDs(part) {
			sets[key][partitionID] = struct{}{}
		}
	}
	out := make(map[string][]string, len(sets))
	for key, set := range sets {
		partitionIDs := make([]string, 0, len(set))
		for partitionID := range set {
			partitionIDs = append(partitionIDs, partitionID)
		}
		sort.Strings(partitionIDs)
		out[key] = partitionIDs
	}
	return out
}

func matchesCompactClaimOptions(part Part, opts CompactClaimOptions) bool {
	if _, excluded := opts.ExcludedJobIDs[part.JobID]; excluded {
		return false
	}
	if opts.JobID != "" && part.JobID != opts.JobID {
		return false
	}
	if opts.Bucket != "" && part.Bucket != opts.Bucket {
		return false
	}
	if opts.DestinationDatabase != "" && part.DestinationDatabase != opts.DestinationDatabase {
		return false
	}
	if opts.DestinationTable != "" && part.DestinationTable != opts.DestinationTable {
		return false
	}
	if opts.DestinationSchema != "" && part.DestinationSchema != opts.DestinationSchema {
		return false
	}
	if len(opts.RequiredPartitionIDs) > 0 && !partOverlapsRequiredPartitions(part, opts.RequiredPartitionIDs) {
		return false
	}
	return true
}

func compactHeartbeatTime(part Part) (time.Time, error) {
	for _, value := range []string{part.UpdatedAt, part.CompactingAt} {
		if strings.TrimSpace(value) == "" {
			continue
		}
		t, err := time.Parse(timeFormat, value)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse compact heartbeat time for part %s/%s: %w", part.JobID, part.PartID, err)
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("compacting part %s/%s has no updated_at or compacting_at", part.JobID, part.PartID)
}

func compactStaleTime(part Part) (time.Time, error) {
	var staleAt time.Time
	for _, value := range []string{part.UpdatedAt, part.CompactingAt} {
		if strings.TrimSpace(value) == "" {
			continue
		}
		t, err := time.Parse(timeFormat, value)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse compact stale time for part %s/%s: %w", part.JobID, part.PartID, err)
		}
		if staleAt.IsZero() || t.Before(staleAt) {
			staleAt = t
		}
	}
	if staleAt.IsZero() {
		return time.Time{}, fmt.Errorf("compacting part %s/%s has no updated_at or compacting_at", part.JobID, part.PartID)
	}
	return staleAt, nil
}

func compactReadyAtForRelease(part Part, now time.Time) string {
	for _, value := range []string{part.CompactReadyAt, part.ProgressUpdatedAt, part.UpdatedAt, part.CompactingAt} {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return formatTime(now)
}

func selectCompactBatchParts(group compactGroup, opts CompactClaimOptions) []Part {
	minParts := opts.MinInputParts
	if minParts == 0 {
		minParts = 2
	}
	partitions := orderedCandidatePartitions(group.parts, opts.RequiredPartitionIDs)
	preferredPartitions := partitionsWithout(partitions, group.compactingPartitionIDs)
	if part, ok := selectFragmentedCompactPart(group.parts, preferredPartitions); ok {
		return []Part{part}
	}
	fallbackPartitions := partitionsWithout(partitions, preferredPartitions)
	if part, ok := selectFragmentedCompactPart(group.parts, fallbackPartitions); ok {
		return []Part{part}
	}
	selected, _, _ := selectCompactBatchPartsForPartitions(group.parts, preferredPartitions, minParts, opts, nil, nil, 0)
	if len(selected) == 0 {
		selected, _, _ = selectCompactBatchPartsForPartitions(group.parts, fallbackPartitions, minParts, opts, nil, nil, 0)
		return selected
	}
	return selected
}

func selectFragmentedCompactPart(parts []Part, partitions []string) (Part, bool) {
	for _, part := range parts {
		eligible := false
		for _, partitionID := range partitions {
			if part.DestinationActivePartitionCounts[partitionID] > 0 {
				eligible = true
				break
			}
		}
		if !eligible {
			continue
		}
		for _, count := range part.DestinationActivePartitionCounts {
			if count > 1 {
				return part, true
			}
		}
	}
	return Part{}, false
}

func selectCompactBatchPartsForPartitions(parts []Part, partitions []string, minParts uint64, opts CompactClaimOptions, selected []Part, selectedIDs map[string]struct{}, inputBytes uint64) ([]Part, map[string]struct{}, uint64) {
	if selectedIDs == nil {
		selectedIDs = map[string]struct{}{}
		for _, part := range selected {
			selectedIDs[part.PartID] = struct{}{}
			inputBytes += part.DestinationActivePartBytes
		}
	}
	for _, partitionID := range partitions {
		additions, addedBytes := selectCompactBatchPartsForPartition(parts, partitionID, minParts, opts, selected, selectedIDs, inputBytes)
		if len(additions) == 0 {
			continue
		}
		for _, part := range additions {
			selected = append(selected, part)
			selectedIDs[part.PartID] = struct{}{}
		}
		inputBytes += addedBytes
		if opts.MaxArtifacts > 0 && len(selected) >= opts.MaxArtifacts {
			break
		}
	}
	return selected, selectedIDs, inputBytes
}

func partitionsWithout(partitions, excluded []string) []string {
	excludedSet := partitionSet(excluded)
	if len(excludedSet) == 0 {
		return append([]string(nil), partitions...)
	}
	out := make([]string, 0, len(partitions))
	for _, partitionID := range partitions {
		if _, ok := excludedSet[partitionID]; ok {
			continue
		}
		out = append(out, partitionID)
	}
	return out
}

func orderedCandidatePartitions(parts []Part, required []string) []string {
	requiredSet := partitionSet(required)
	seen := map[string]struct{}{}
	var partitions []string
	for _, part := range parts {
		ids := partPartitionIDs(part)
		for _, partitionID := range ids {
			if len(requiredSet) > 0 {
				if _, ok := requiredSet[partitionID]; !ok {
					continue
				}
			}
			if _, ok := seen[partitionID]; ok {
				continue
			}
			seen[partitionID] = struct{}{}
			partitions = append(partitions, partitionID)
		}
	}
	return partitions
}

func selectCompactBatchPartsForPartition(parts []Part, partitionID string, minParts uint64, opts CompactClaimOptions, selected []Part, selectedIDs map[string]struct{}, inputBytes uint64) ([]Part, uint64) {
	var additions []Part
	var partitionInputParts, addedBytes uint64
	for _, part := range selected {
		partitionInputParts += part.DestinationActivePartitionCounts[partitionID]
	}
	if partitionInputParts >= minParts {
		return nil, 0
	}
	appendCandidate := func(part Part) bool {
		if _, ok := selectedIDs[part.PartID]; ok {
			return false
		}
		partitionParts := part.DestinationActivePartitionCounts[partitionID]
		if partitionParts == 0 {
			return false
		}
		if opts.MaxArtifacts > 0 && len(selected)+len(additions) >= opts.MaxArtifacts {
			return true
		}
		partBytes := part.DestinationActivePartBytes
		if opts.MaxBytes > 0 && inputBytes+addedBytes+partBytes > opts.MaxBytes && len(selected)+len(additions) > 0 {
			return true
		}
		additions = append(additions, part)
		partitionInputParts += partitionParts
		addedBytes += partBytes
		return opts.MaxArtifacts > 0 && len(selected)+len(additions) >= opts.MaxArtifacts
	}
	for _, part := range parts {
		if appendCandidate(part) {
			break
		}
	}
	if partitionInputParts >= minParts {
		return additions, addedBytes
	}
	return nil, 0
}

func partitionSet(partitionIDs []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, partitionID := range partitionIDs {
		if strings.TrimSpace(partitionID) == "" {
			continue
		}
		out[partitionID] = struct{}{}
	}
	return out
}

func partOverlapsRequiredPartitions(part Part, required []string) bool {
	for partitionID := range partitionSet(required) {
		if part.DestinationActivePartitionCounts[partitionID] > 0 {
			return true
		}
	}
	return false
}

func partPartitionIDs(part Part) []string {
	ids := make([]string, 0, len(part.DestinationActivePartitionCounts))
	for partitionID, count := range part.DestinationActivePartitionCounts {
		if strings.TrimSpace(partitionID) == "" || count == 0 {
			continue
		}
		ids = append(ids, partitionID)
	}
	sort.Strings(ids)
	return ids
}

func (s *Store) claimCompactParts(ctx context.Context, parts []Part, workerID string, now time.Time) ([]Part, error) {
	if err := validateCompactBatchParts(parts); err != nil {
		return nil, err
	}
	for _, part := range parts {
		if part.Status != StatusCompactReady {
			return nil, fmt.Errorf("compact batch part %s/%s is %s, expected %s", part.JobID, part.PartID, part.Status, StatusCompactReady)
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	claimed := make([]Part, 0, len(parts))
	for _, part := range parts {
		claimedPart, err := s.readPartTx(ctx, tx, part.JobID, part.PartID)
		if err != nil {
			return nil, fmt.Errorf("claim compact-ready part %s/%s: %w", part.JobID, part.PartID, err)
		}
		if claimedPart.Status != StatusCompactReady {
			return nil, fmt.Errorf("claim compact-ready part %s/%s: %w", part.JobID, part.PartID, &conditionalCheckFailedError{})
		}
		setStatus(&claimedPart, StatusCompacting, now)
		claimedPart.CompactingAt = formatTime(now)
		claimedPart.WorkerID = workerID
		claimedPart.Error = ""
		claimedPart.CompactCooldownUntil = ""
		if err := s.savePartTx(ctx, tx, claimedPart); err != nil {
			return nil, fmt.Errorf("claim compact-ready part %s/%s: %w", part.JobID, part.PartID, err)
		}
		claimed = append(claimed, claimedPart)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

func compactBatchFromParts(parts []Part) (*CompactBatch, error) {
	if len(parts) == 0 {
		return nil, nil
	}
	if err := validateCompactBatchParts(parts); err != nil {
		return nil, err
	}
	batch := &CompactBatch{
		JobID: parts[0].JobID,
		Parts: append([]Part(nil), parts...),
	}
	for _, part := range parts {
		batch.InputPartCount += part.DestinationActivePartCount
		batch.InputRows += part.DestinationActivePartRows
		batch.InputBytes += part.DestinationActivePartBytes
		if part.CompactGeneration >= batch.Generation {
			batch.Generation = part.CompactGeneration + 1
		}
	}
	return batch, nil
}

func validateCompactBatch(batch CompactBatch) error {
	if err := validateCompactBatchParts(batch.Parts); err != nil {
		return err
	}
	if strings.TrimSpace(batch.JobID) == "" {
		return errors.New("compact batch job id is required")
	}
	if batch.JobID != batch.Parts[0].JobID {
		return fmt.Errorf("compact batch job id %q does not match input job id %q", batch.JobID, batch.Parts[0].JobID)
	}
	return nil
}

func validateCompactBatchParts(parts []Part) error {
	if len(parts) == 0 {
		return errors.New("compact batch has no input parts")
	}
	first := parts[0]
	if err := validateCompactBatchPart(first); err != nil {
		return err
	}
	for _, part := range parts[1:] {
		if err := validateCompactBatchPart(part); err != nil {
			return err
		}
		if part.JobID != first.JobID {
			return fmt.Errorf("compact batch mixes job ids %q and %q", first.JobID, part.JobID)
		}
		if part.Bucket != first.Bucket {
			return fmt.Errorf("compact batch for job %s mixes buckets %q and %q", first.JobID, first.Bucket, part.Bucket)
		}
		if part.DestinationDatabase != first.DestinationDatabase ||
			part.DestinationTable != first.DestinationTable ||
			part.DestinationSchema != first.DestinationSchema {
			return fmt.Errorf("compact batch for job %s mixes destinations", first.JobID)
		}
	}
	return nil
}

func validateCompactBatchPart(part Part) error {
	if err := validatePart(part); err != nil {
		return err
	}
	if strings.TrimSpace(part.DestinationDatabase) == "" ||
		strings.TrimSpace(part.DestinationTable) == "" ||
		strings.TrimSpace(part.DestinationSchema) == "" {
		return fmt.Errorf("compact batch part %s/%s is missing destination database, table, or schema", part.JobID, part.PartID)
	}
	return nil
}

func validateCompactOutputForBatch(batch CompactBatch, output Part) error {
	input := batch.Parts[0]
	if output.JobID != batch.JobID {
		return fmt.Errorf("compact output job id %q does not match batch job id %q", output.JobID, batch.JobID)
	}
	if output.Bucket != input.Bucket {
		return fmt.Errorf("compact output %s/%s bucket %q does not match input bucket %q", output.JobID, output.PartID, output.Bucket, input.Bucket)
	}
	if output.DestinationDatabase != input.DestinationDatabase ||
		output.DestinationTable != input.DestinationTable ||
		output.DestinationSchema != input.DestinationSchema {
		return fmt.Errorf("compact output %s/%s destination does not match batch destination", output.JobID, output.PartID)
	}
	return nil
}

func (s *Store) MarkFinished(ctx context.Context, part Part, workerID, finishedKey string, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if strings.TrimSpace(finishedKey) == "" {
		return errors.New("finished key is required")
	}
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusInProgress && current.WorkerID == workerID
	}, func(current *Part) error {
		setStatus(current, StatusFinished, now)
		current.FinishedAt = formatTime(now)
		current.FinishedKey = finishedKey
		current.WorkerID = ""
		current.Error = ""
		return nil
	})
	if err != nil {
		return fmt.Errorf("mark part %s/%s finished: %w", part.JobID, part.PartID, err)
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, part Part, workerID string, cause error, now time.Time) error {
	if cause == nil {
		return errors.New("failure cause is required")
	}
	return s.transitionOwned(ctx, part, workerID, StatusFailed, "failed_at", cause.Error(), now)
}

func (s *Store) ReleaseInProgress(ctx context.Context, part Part, workerID string, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusInProgress && current.WorkerID == workerID
	}, func(current *Part) error {
		setStatus(current, StatusReady, now)
		current.WorkerID = ""
		current.StartedAt = ""
		current.Error = ""
		clearRewriteProgress(current)
		return nil
	})
	if err != nil {
		return fmt.Errorf("release state item for %s/%s back to %s: %w", part.JobID, part.PartID, StatusReady, err)
	}
	return nil
}

func (s *Store) UpdateRewriteProgress(ctx context.Context, jobID, partID, workerID string, progress RewriteProgress, now time.Time) error {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(partID) == "" {
		return errors.New("job id and part id are required")
	}
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	_, err := s.updatePart(ctx, jobID, partID, func(current Part) bool {
		return current.Status == StatusInProgress && current.WorkerID == workerID
	}, func(current *Part) error {
		current.UpdatedAt = formatTime(now)
		current.ProgressUpdatedAt = formatTime(now)
		if progress.QueryProgress != nil {
			current.ReadRows = progress.QueryProgress.ReadRows
			current.ReadBytes = progress.QueryProgress.ReadBytes
			current.WrittenRows = progress.QueryProgress.WrittenRows
			current.WrittenBytes = progress.QueryProgress.WrittenBytes
		}
		if progress.SourceActivePartStats != nil {
			current.SourceActivePartCount = progress.SourceActivePartStats.Count
			current.SourceActivePartRows = progress.SourceActivePartStats.Rows
			current.SourceActivePartBytes = progress.SourceActivePartStats.Bytes
		}
		if progress.DestinationActivePartStats != nil {
			current.DestinationActivePartCount = progress.DestinationActivePartStats.Count
			current.DestinationActivePartRows = progress.DestinationActivePartStats.Rows
			current.DestinationActivePartBytes = progress.DestinationActivePartStats.Bytes
		}
		if progress.DestinationFailedMerges != nil {
			current.DestinationFailedMerges = *progress.DestinationFailedMerges
		}
		if progress.StageProgress != nil {
			current.RewriteStage = progress.StageProgress.Stage
			current.RewriteStageStartedAt = formatTime(progress.StageProgress.StageStartedAt)
			current.RewriteStageElapsedMs = progress.StageProgress.StageElapsedMs
			current.RewriteTotalElapsedMs = progress.StageProgress.TotalElapsedMs
			current.RewriteStageDurationsMs = progress.StageProgress.CompletedStageDurationsMs
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("update rewrite progress for %s/%s: %w", jobID, partID, err)
	}
	return nil
}

func (s *Store) ListJobIDs(ctx context.Context) ([]string, error) {
	return s.ListJobIDsByStatus(ctx, allStatuses...)
}

func (s *Store) ListJobs(ctx context.Context) ([]Job, error) {
	return s.ListJobsByStatus(ctx, allStatuses...)
}

func (s *Store) ListJobIDsByStatus(ctx context.Context, statuses ...Status) ([]string, error) {
	jobs, err := s.ListJobsByStatus(ctx, statuses...)
	if err != nil {
		return nil, err
	}
	jobIDs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		jobIDs = append(jobIDs, job.JobID)
	}
	return jobIDs, nil
}

func (s *Store) ListJobsByStatus(ctx context.Context, statuses ...Status) ([]Job, error) {
	jobsByID := map[string]Job{}
	jobPartitionsByID := map[string]map[string]struct{}{}
	queried := map[Status]struct{}{}
	for _, status := range statuses {
		if strings.TrimSpace(string(status)) == "" {
			return nil, errors.New("status is required")
		}
		if _, ok := queried[status]; ok {
			continue
		}
		queried[status] = struct{}{}
		parts, err := s.listPartsByStatusIndex(ctx, status)
		if err != nil {
			return nil, fmt.Errorf("query job ids for status %s: %w", status, err)
		}
		for _, part := range parts {
			if part.JobID == "" {
				continue
			}
			existing := jobsByID[part.JobID]
			if existing.JobID == "" {
				existing = Job{JobID: part.JobID, Name: part.JobName, Counts: map[Status]int{}}
			}
			if existing.Name == "" && part.JobName != "" {
				existing.Name = part.JobName
			}
			if existing.Name != "" && part.JobName != "" && existing.Name != part.JobName {
				return nil, fmt.Errorf("job %s has conflicting job_name values %q and %q", part.JobID, existing.Name, part.JobName)
			}
			existing.Total++
			existing.Counts[status]++
			if status != StatusSuperseded {
				existing.DestinationActivePartCount += part.DestinationActivePartCount
				if jobPartitionsByID[part.JobID] == nil {
					jobPartitionsByID[part.JobID] = map[string]struct{}{}
				}
				for partitionID, count := range part.DestinationActivePartitionCounts {
					if strings.TrimSpace(partitionID) != "" && count > 0 {
						jobPartitionsByID[part.JobID][partitionID] = struct{}{}
					}
				}
				existing.DestinationPartitionCount = len(jobPartitionsByID[part.JobID])
			}
			if part.CreatedAt != "" && (existing.SubmittedAt == "" || part.CreatedAt < existing.SubmittedAt) {
				existing.SubmittedAt = part.CreatedAt
			}
			if part.UpdatedAt != "" && part.UpdatedAt > existing.UpdatedAt {
				existing.UpdatedAt = part.UpdatedAt
			}
			jobsByID[part.JobID] = existing
		}
	}

	jobs := make([]Job, 0, len(jobsByID))
	for _, job := range jobsByID {
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].JobID < jobs[j].JobID
	})
	return jobs, nil
}

func (s *Store) ListJobParts(ctx context.Context, jobID string) ([]Part, error) {
	if strings.TrimSpace(jobID) == "" {
		return nil, errors.New("job id is required")
	}
	rows, err := s.pool.Query(ctx, `SELECT data FROM `+s.tableSQL+` WHERE job_id = $1 ORDER BY part_id`, jobID)
	if err != nil {
		return nil, fmt.Errorf("query job parts for %s: %w", jobID, err)
	}
	defer rows.Close()
	var parts []Part
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		part, err := partFromJSON(data)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartID < parts[j].PartID
	})
	return parts, nil
}

func (s *Store) ListFinishedParts(ctx context.Context, jobID string) ([]Part, error) {
	allParts, err := s.ListJobParts(ctx, jobID)
	if err != nil {
		return nil, err
	}
	var parts []Part
	for _, part := range allParts {
		if part.Status == StatusFinished {
			parts = append(parts, part)
		}
	}
	return parts, nil
}

func (s *Store) DeleteJobParts(ctx context.Context, parts []Part) error {
	return s.DeleteJobPartsAfterLock(ctx, parts, nil)
}

func (s *Store) DeleteJobPartsAfterLock(ctx context.Context, parts []Part, afterLock func() error) error {
	if len(parts) == 0 {
		return errors.New("job has no parts to delete")
	}
	jobID := parts[0].JobID
	if strings.TrimSpace(jobID) == "" {
		return errors.New("job id is required")
	}
	for _, part := range parts {
		if err := validatePart(part); err != nil {
			return err
		}
		if part.JobID != jobID {
			return fmt.Errorf("delete job parts got mixed job ids %q and %q", jobID, part.JobID)
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, part := range parts {
		if _, err := s.readPartTx(ctx, tx, part.JobID, part.PartID); err != nil {
			return fmt.Errorf("delete state item for %s/%s: %w", part.JobID, part.PartID, err)
		}
		ref, ok, err := s.sourceDependentTx(ctx, tx, part.JobID, part.PartID)
		if err != nil {
			return fmt.Errorf("check source part dependents for %s/%s: %w", part.JobID, part.PartID, err)
		}
		if ok {
			return fmt.Errorf("cannot delete source part %s/%s; it is referenced by %s/%s", part.JobID, part.PartID, ref.JobID, ref.PartID)
		}
	}
	if afterLock != nil {
		if err := afterLock(); err != nil {
			return err
		}
	}
	for _, part := range parts {
		tag, err := tx.Exec(ctx, `DELETE FROM `+s.tableSQL+` WHERE job_id = $1 AND part_id = $2`, part.JobID, part.PartID)
		if err != nil {
			return fmt.Errorf("delete state item for %s/%s: %w", part.JobID, part.PartID, err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("delete state item for %s/%s: %w", part.JobID, part.PartID, &conditionalCheckFailedError{})
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) sourceDependentTx(ctx context.Context, tx pgx.Tx, jobID, partID string) (Part, bool, error) {
	var data []byte
	err := tx.QueryRow(ctx, `SELECT data FROM `+s.tableSQL+` WHERE data->>'source_job_id' = $1 AND data->>'source_part_id' = $2 ORDER BY job_id, part_id LIMIT 1`, jobID, partID).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Part{}, false, nil
	}
	if err != nil {
		return Part{}, false, err
	}
	part, err := partFromJSON(data)
	if err != nil {
		return Part{}, false, err
	}
	return part, true, nil
}

func (s *Store) MarkImporting(ctx context.Context, part Part, now time.Time) error {
	return s.transition(ctx, part, StatusFinished, StatusImporting, "importing_at", "", now)
}

func (s *Store) MarkImported(ctx context.Context, part Part, now time.Time) error {
	return s.transition(ctx, part, StatusImporting, StatusImported, "imported_at", "", now)
}

func (s *Store) ReleaseImport(ctx context.Context, part Part, now time.Time) error {
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusImporting
	}, func(current *Part) error {
		setStatus(current, StatusFinished, now)
		current.ImportingAt = ""
		current.Error = ""
		return nil
	})
	if err != nil {
		return fmt.Errorf("release import for %s/%s: %w", part.JobID, part.PartID, err)
	}
	return nil
}

func (s *Store) MarkImportFailed(ctx context.Context, part Part, cause error, now time.Time) error {
	if cause == nil {
		return errors.New("failure cause is required")
	}
	return s.transition(ctx, part, StatusImporting, StatusFailed, "failed_at", cause.Error(), now)
}

func (s *Store) RetryFailedPart(ctx context.Context, part Part, now time.Time) (Status, error) {
	if part.Status != StatusFailed {
		return "", fmt.Errorf("part %s/%s is %s, expected %s", part.JobID, part.PartID, part.Status, StatusFailed)
	}
	target := failedRetryTarget(part)
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusFailed
	}, func(current *Part) error {
		setStatus(current, target, now)
		current.Error = ""
		current.FailedAt = ""
		current.ImportingAt = ""
		current.ImportedAt = ""
		current.WorkerID = ""
		if target == StatusReady {
			current.StartedAt = ""
			current.FinishedAt = ""
			clearRewriteProgress(current)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("retry failed state item for %s/%s as %s: %w", part.JobID, part.PartID, target, err)
	}
	return target, nil
}

func failedRetryTarget(part Part) Status {
	if part.ImportingAt != "" {
		return StatusFinished
	}
	if part.CompactReadyAt != "" {
		return StatusCompactReady
	}
	return StatusReady
}

func (s *Store) RetryInProgressPart(ctx context.Context, part Part, now time.Time) (Status, error) {
	if part.Status != StatusInProgress {
		return "", fmt.Errorf("part %s/%s is %s, expected %s", part.JobID, part.PartID, part.Status, StatusInProgress)
	}
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusInProgress
	}, func(current *Part) error {
		setStatus(current, StatusReady, now)
		current.Error = ""
		current.StartedAt = ""
		current.WorkerID = ""
		clearRewriteProgress(current)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("retry in-progress state item for %s/%s as %s: %w", part.JobID, part.PartID, StatusReady, err)
	}
	return StatusReady, nil
}

func (s *Store) RetryStaleInProgressPart(ctx context.Context, part Part, now time.Time) (Status, error) {
	if part.Status != StatusInProgress {
		return "", fmt.Errorf("part %s/%s is %s, expected %s", part.JobID, part.PartID, part.Status, StatusInProgress)
	}
	if strings.TrimSpace(part.ProgressUpdatedAt) == "" {
		return "", fmt.Errorf("part %s/%s has no progress_updated_at", part.JobID, part.PartID)
	}
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusInProgress && current.ProgressUpdatedAt == part.ProgressUpdatedAt
	}, func(current *Part) error {
		setStatus(current, StatusReady, now)
		current.Error = ""
		current.StartedAt = ""
		current.WorkerID = ""
		clearRewriteProgress(current)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("retry stale in-progress state item for %s/%s as %s: %w", part.JobID, part.PartID, StatusReady, err)
	}
	return StatusReady, nil
}

func (s *Store) ForceRetryPart(ctx context.Context, part Part, now time.Time) (Status, error) {
	_, err := s.updatePart(ctx, part.JobID, part.PartID, nil, func(current *Part) error {
		setStatus(current, StatusReady, now)
		current.Error = ""
		current.FailedAt = ""
		current.StartedAt = ""
		current.FinishedAt = ""
		current.ImportingAt = ""
		current.ImportedAt = ""
		current.WorkerID = ""
		clearRewriteProgress(current)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("force retry state item for %s/%s: %w", part.JobID, part.PartID, err)
	}
	return StatusReady, nil
}

func (s *Store) ForceSetPartStatus(ctx context.Context, part Part, to Status, now time.Time) error {
	if err := validatePart(part); err != nil {
		return err
	}
	if strings.TrimSpace(part.UpdatedAt) == "" {
		return fmt.Errorf("part %s/%s is missing updated_at", part.JobID, part.PartID)
	}

	removeRewriteProgress := false
	switch to {
	case StatusReady:
		removeRewriteProgress = true
	case StatusCompactReady:
	case StatusFinished:
	default:
		return fmt.Errorf("cannot force set part %s/%s to %s", part.JobID, part.PartID, to)
	}

	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.UpdatedAt == part.UpdatedAt
	}, func(current *Part) error {
		setStatus(current, to, now)
		current.Error = ""
		current.StartedAt = ""
		current.CompactingAt = ""
		current.ImportingAt = ""
		current.ImportedAt = ""
		current.FailedAt = ""
		current.WorkerID = ""
		current.CompactCooldownUntil = ""
		clearCompactProgress(current)
		if removeRewriteProgress {
			clearRewriteProgress(current)
		}
		switch to {
		case StatusReady:
			current.FinishedAt = ""
			current.CompactReadyAt = ""
			current.SupersededAt = ""
			current.SupersededBy = ""
		case StatusCompactReady:
			if strings.TrimSpace(current.CompactReadyAt) == "" {
				current.CompactReadyAt = formatTime(now)
			}
			current.FinishedAt = ""
			current.SupersededAt = ""
			current.SupersededBy = ""
		case StatusFinished:
			if strings.TrimSpace(current.FinishedAt) == "" {
				current.FinishedAt = formatTime(now)
			}
			current.SupersededAt = ""
			current.SupersededBy = ""
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("force set part %s/%s to %s: %w", part.JobID, part.PartID, to, err)
	}
	return nil
}

func (s *Store) ResetCompactTimer(ctx context.Context, part Part, now time.Time) error {
	if err := validatePart(part); err != nil {
		return err
	}
	_, err := s.updatePart(ctx, part.JobID, part.PartID, nil, func(current *Part) error {
		current.CompactReadyAt = formatTime(now)
		current.CompactCooldownUntil = ""
		return nil
	})
	if err != nil {
		return fmt.Errorf("reset compact timer for %s/%s: %w", part.JobID, part.PartID, err)
	}
	return nil
}

func (s *Store) ResetOriginalPartToReady(ctx context.Context, part Part, now time.Time) error {
	if err := validateOriginalResetPart(part); err != nil {
		return err
	}
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.UpdatedAt == part.UpdatedAt
	}, func(current *Part) error {
		setStatus(current, StatusReady, now)
		current.Error = ""
		current.FailedAt = ""
		current.StartedAt = ""
		current.FinishedAt = ""
		current.CompactReadyAt = ""
		current.CompactingAt = ""
		current.SupersededAt = ""
		current.ImportingAt = ""
		current.ImportedAt = ""
		current.WorkerID = ""
		current.CompactCooldownUntil = ""
		current.SupersededBy = ""
		if strings.TrimSpace(current.SourceJobID) == "" {
			current.DestinationDatabase = ""
			current.DestinationTable = ""
			current.DestinationSchema = ""
			current.InsertSelect = ""
		}
		current.CompactGeneration = 0
		current.CompactInputPartIDs = nil
		clearCompactProgress(current)
		clearRewriteProgress(current)
		return nil
	})
	if err != nil {
		return fmt.Errorf("reset original part %s/%s to %s: %w", part.JobID, part.PartID, StatusReady, err)
	}
	return nil
}

func (s *Store) ResetOriginalPartToCompactReady(ctx context.Context, part Part, now time.Time) error {
	if err := validateOriginalResetPart(part); err != nil {
		return err
	}
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.UpdatedAt == part.UpdatedAt
	}, func(current *Part) error {
		setStatus(current, StatusCompactReady, now)
		current.CompactReadyAt = formatTime(now)
		current.Error = ""
		current.FailedAt = ""
		current.StartedAt = ""
		current.FinishedAt = ""
		current.CompactingAt = ""
		current.SupersededAt = ""
		current.ImportingAt = ""
		current.ImportedAt = ""
		current.WorkerID = ""
		current.CompactCooldownUntil = ""
		current.SupersededBy = ""
		current.CompactInputPartIDs = nil
		clearCompactProgress(current)
		return nil
	})
	if err != nil {
		return fmt.Errorf("reset original part %s/%s to %s: %w", part.JobID, part.PartID, StatusCompactReady, err)
	}
	return nil
}

func validateOriginalResetPart(part Part) error {
	if err := validatePart(part); err != nil {
		return err
	}
	if len(part.CompactInputPartIDs) > 0 || part.CompactGeneration > 0 {
		return fmt.Errorf("part %s/%s is a generated compact part, not an original source part", part.JobID, part.PartID)
	}
	if strings.TrimSpace(part.UpdatedAt) == "" {
		return fmt.Errorf("part %s/%s is missing updated_at", part.JobID, part.PartID)
	}
	return nil
}

func (s *Store) claimPart(ctx context.Context, part Part, workerID string, now time.Time) (*Part, error) {
	claimed, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusReady
	}, func(current *Part) error {
		claimPartInMemory(current, workerID, now)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("claim state item for %s/%s: %w", part.JobID, part.PartID, err)
	}
	return &claimed, nil
}

func claimPartInMemory(part *Part, workerID string, now time.Time) {
	setStatus(part, StatusInProgress, now)
	part.StartedAt = formatTime(now)
	part.WorkerID = workerID
	part.Attempts++
	part.Error = ""
	clearRewriteProgress(part)
}

func (s *Store) transitionOwned(ctx context.Context, part Part, workerID string, to Status, timestampAttr, errorText string, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == StatusInProgress && current.WorkerID == workerID
	}, func(current *Part) error {
		setStatus(current, to, now)
		setTimestampAttr(current, timestampAttr, formatTime(now))
		if errorText != "" {
			current.Error = errorText
		} else {
			current.Error = ""
		}
		current.WorkerID = ""
		return nil
	})
	if err != nil {
		return fmt.Errorf("transition state item for %s/%s to %s: %w", part.JobID, part.PartID, to, err)
	}
	return nil
}

func (s *Store) transition(ctx context.Context, part Part, from, to Status, timestampAttr, errorText string, now time.Time) error {
	_, err := s.updatePart(ctx, part.JobID, part.PartID, func(current Part) bool {
		return current.Status == from
	}, func(current *Part) error {
		setStatus(current, to, now)
		setTimestampAttr(current, timestampAttr, formatTime(now))
		current.Error = errorText
		return nil
	})
	if err != nil {
		return fmt.Errorf("transition state item for %s/%s from %s to %s: %w", part.JobID, part.PartID, from, to, err)
	}
	return nil
}

func IsConditionalCheckFailed(err error) bool {
	var conditional *conditionalCheckFailedError
	return errors.As(err, &conditional)
}

func validatePart(part Part) error {
	if part.JobID == "" || part.PartID == "" || part.Bucket == "" || part.SourceKey == "" || part.FinishedKey == "" {
		return errors.New("part state is missing job_id, part_id, bucket, source_key, or finished_key")
	}
	if part.Status == "" {
		return errors.New("part state is missing status")
	}
	if (part.SourceJobID == "") != (part.SourcePartID == "") {
		return errors.New("part state source_job_id and source_part_id must be set together")
	}
	if part.SourceJobID == part.JobID && part.SourcePartID == part.PartID {
		return fmt.Errorf("part %s/%s cannot reference itself as a source part", part.JobID, part.PartID)
	}
	return nil
}

func isGeneratedCompactPart(part Part) bool {
	return len(part.CompactInputPartIDs) > 0 || part.CompactGeneration > 0
}

func formatTime(t time.Time) string {
	return t.UTC().Format(timeFormat)
}

func setTimestampAttr(part *Part, name, value string) {
	switch name {
	case "started_at":
		part.StartedAt = value
	case "finished_at":
		part.FinishedAt = value
	case "compact_ready_at":
		part.CompactReadyAt = value
	case "compacting_at":
		part.CompactingAt = value
	case "superseded_at":
		part.SupersededAt = value
	case "importing_at":
		part.ImportingAt = value
	case "imported_at":
		part.ImportedAt = value
	case "failed_at":
		part.FailedAt = value
	default:
		panic(fmt.Sprintf("unknown timestamp attribute %q", name))
	}
}
