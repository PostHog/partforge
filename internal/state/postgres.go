package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	rdsauth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
	tableName         string
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

	SourceArtifactBytes  uint64   `json:"source_artifact_bytes,omitempty"`
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
	TotalRowsApprox                  uint64            `json:"total_rows_approx,omitempty"`
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
	ReadRows        uint64
	ReadBytes       uint64
	TotalRowsApprox uint64
	WrittenRows     uint64
	WrittenBytes    uint64
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
	CompactWindow        time.Duration
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
	store, err := openStore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := store.checkSchema(ctx); err != nil {
		store.pool.Close()
		return nil, err
	}
	return store, nil
}

func openStore(ctx context.Context, cfg Config) (*Store, error) {
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
		tableName:         strings.TrimSpace(cfg.Table),
		tableSQL:          tableSQL,
		statusIndexSQL:    statusIndexSQL,
		jobStatusIndexSQL: jobStatusIndexSQL,
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

const partColumns = "job_id, part_id, status, worker_id, created_at, updated_at, data, source_artifact_bytes, compact_bytes, compact_eligible, compact_normalized, compact_stale_at, original_compact_ready_at"

// Full writes derive scheduling values once and persist them alongside the JSON.
func partWriteValues(part Part) ([]any, error) {
	data, err := partJSON(part)
	if err != nil {
		return nil, err
	}
	partitions, fragmented, single := 0, false, true
	for id, count := range part.DestinationActivePartitionCounts {
		if strings.TrimSpace(id) == "" || count == 0 {
			continue
		}
		partitions++
		fragmented = fragmented || count > 1
		single = single && count == 1
	}
	eligible := strings.TrimSpace(part.DestinationDatabase) != "" && strings.TrimSpace(part.DestinationTable) != "" && strings.TrimSpace(part.DestinationSchema) != "" && part.DestinationActivePartCount > 0 && fragmented
	normalized := part.DestinationActivePartCount == 1 && partitions == 1 && single
	var staleAt, originalReadyAt *time.Time
	if part.Status == StatusCompacting {
		for _, value := range []string{part.UpdatedAt, part.CompactingAt} {
			if strings.TrimSpace(value) == "" {
				continue
			}
			parsed, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return nil, fmt.Errorf("compact time for %s/%s: %w", part.JobID, part.PartID, err)
			}
			if staleAt == nil || parsed.Before(*staleAt) {
				staleAt = &parsed
			}
		}
		if staleAt == nil {
			return nil, fmt.Errorf("compacting part %s/%s has no updated_at or compacting_at", part.JobID, part.PartID)
		}
	}
	if !isGeneratedCompactPart(part) && strings.TrimSpace(part.CompactReadyAt) != "" {
		parsed, err := time.Parse(time.RFC3339Nano, part.CompactReadyAt)
		if err != nil {
			return nil, fmt.Errorf("compact-ready time for %s/%s: %w", part.JobID, part.PartID, err)
		}
		originalReadyAt = &parsed
	}
	return []any{part.JobID, part.PartID, string(part.Status), part.WorkerID, part.CreatedAt, part.UpdatedAt, data,
		pgtype.Numeric{Int: new(big.Int).SetUint64(part.SourceArtifactBytes), Valid: true},
		pgtype.Numeric{Int: new(big.Int).SetUint64(part.DestinationActivePartBytes), Valid: true},
		eligible, normalized, staleAt, originalReadyAt}, nil
}

func (s *Store) insertPartTx(ctx context.Context, tx pgx.Tx, part Part) error {
	values, err := partWriteValues(part)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO `+s.tableSQL+` (`+partColumns+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, values...)
	return err
}

func (s *Store) savePartTx(ctx context.Context, tx pgx.Tx, part Part) error {
	values, err := partWriteValues(part)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE `+s.tableSQL+` SET status=$3, worker_id=$4, created_at=$5, updated_at=$6, data=$7,
 source_artifact_bytes=$8, compact_bytes=$9, compact_eligible=$10, compact_normalized=$11,
 compact_stale_at=$12, original_compact_ready_at=$13 WHERE job_id=$1 AND part_id=$2`, values...)
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
	part.TotalRowsApprox = 0
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
	err = s.insertPartTx(ctx, tx, part)
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

func (s *Store) readyClaimQuery() string {
	return `SELECT data FROM ` + s.tableSQL + ` WHERE status = 'READY' ORDER BY source_artifact_bytes DESC, created_at, job_id, part_id LIMIT 1 FOR UPDATE SKIP LOCKED`
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
	err = tx.QueryRow(ctx, s.readyClaimQuery()).Scan(&data)
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	query, args := s.compactClaimQuery(opts, now)
	var data []byte
	err = tx.QueryRow(ctx, query, args...).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim compact-ready part: %w", err)
	}
	part, err := partFromJSON(data)
	if err != nil {
		return nil, err
	}
	setStatus(&part, StatusCompacting, now)
	part.CompactingAt = formatTime(now)
	part.WorkerID = workerID
	part.Error = ""
	part.CompactCooldownUntil = ""
	batch, err := compactBatchFromParts([]Part{part})
	if err != nil {
		return nil, err
	}
	if err := s.savePartTx(ctx, tx, part); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return batch, nil
}

// Claim the largest eligible unlocked artifact using the compact queue index.
func (s *Store) compactClaimQuery(opts CompactClaimOptions, now time.Time) (string, []any) {
	args := []any{}
	bind := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }
	filters := []string{"p.status = 'COMPACT_READY'", "p.compact_eligible"}
	for _, filter := range []struct{ field, value string }{
		{"p.job_id", opts.JobID}, {"p.data->>'bucket'", opts.Bucket},
		{"p.data->>'destination_database'", opts.DestinationDatabase},
		{"p.data->>'destination_table'", opts.DestinationTable},
		{"p.data->>'destination_schema'", opts.DestinationSchema},
	} {
		if filter.value != "" {
			filters = append(filters, filter.field+" = "+bind(filter.value))
		}
	}
	if len(opts.ExcludedJobIDs) > 0 {
		ids := make([]string, 0, len(opts.ExcludedJobIDs))
		for id := range opts.ExcludedJobIDs {
			ids = append(ids, id)
		}
		filters = append(filters, "NOT (p.job_id = ANY("+bind(ids)+"::text[]))")
	}
	required := ""
	if len(opts.RequiredPartitionIDs) > 0 {
		required = " AND partition.key = ANY(" + bind(opts.RequiredPartitionIDs) + "::text[])"
		filters = append(filters, "EXISTS (SELECT FROM jsonb_each_text(p.data->'destination_active_partition_counts') partition WHERE btrim(partition.key) <> '' AND partition.value::numeric > 0"+required+")")
	}
	from := s.tableSQL + " p"
	if opts.CompactWindow > 0 {
		cutoff := bind(now.Add(-opts.CompactWindow))
		// A lateral join lets Postgres memoize the deadline by job while walking the queue.
		from += " LEFT JOIN LATERAL (SELECT original_compact_ready_at FROM " + s.tableSQL + " original WHERE original.job_id = p.job_id AND original_compact_ready_at IS NOT NULL ORDER BY original_compact_ready_at DESC LIMIT 1) deadline ON true"
		filters = append(filters, "COALESCE(deadline.original_compact_ready_at > "+cutoff+", true)")
	}
	return "SELECT p.data FROM " + from + " WHERE " + strings.Join(filters, " AND ") + " ORDER BY p.compact_bytes DESC, p.created_at, p.job_id, p.part_id LIMIT 1 FOR UPDATE OF p SKIP LOCKED", args
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
	requested := false
	for _, part := range batch.Parts {
		finalize, err := s.updateCompactProgress(ctx, part, workerID, []byte(`{}`), now)
		if err != nil {
			return false, fmt.Errorf("heartbeat compacting part %s/%s: %w", part.JobID, part.PartID, err)
		}
		requested = requested || finalize
	}
	return requested, nil
}

// One conditional statement preserves ownership and finalization requests while
// patching only progress fields, rather than reading and rewriting the full part.
func (s *Store) updateCompactProgress(ctx context.Context, part Part, workerID string, patch []byte, now time.Time) (bool, error) {
	var requested bool
	err := s.pool.QueryRow(ctx, `UPDATE `+s.tableSQL+` SET status = 'COMPACTING', worker_id = $1, updated_at = $2,
 compact_stale_at = LEAST($2::text::timestamptz, COALESCE(NULLIF(btrim(data->>'compacting_at'), '')::timestamptz, $2::text::timestamptz)),
 data = (data - 'error' - 'compact_cooldown_until') || $3::jsonb ||
 jsonb_build_object('status', 'COMPACTING', 'worker_id', $1::text, 'updated_at', $2::text,
 'compacting_at', COALESCE(NULLIF(btrim(data->>'compacting_at'), ''), $2::text))
 WHERE job_id = $4 AND part_id = $5 AND
 ((status = 'COMPACTING' AND worker_id = $1) OR (status = 'COMPACT_READY' AND btrim(worker_id) = ''))
 RETURNING COALESCE(btrim(data->>'compact_finalize_requested_at'), '') <> ''`, workerID, formatTime(now), patch, part.JobID, part.PartID).Scan(&requested)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, &conditionalCheckFailedError{message: fmt.Sprintf("part %s/%s did not match expected state", part.JobID, part.PartID)}
	}
	return requested, err
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
	patch, err := json.Marshal(map[string]any{
		"compact_progress_at": formatTime(now), "compact_output_part_id": outputPartID,
		"compact_input_part_count": inputStats.Count, "compact_input_rows": inputStats.Rows, "compact_input_bytes": inputStats.Bytes,
		"compact_output_part_count": outputStats.Count, "compact_output_rows": outputStats.Rows, "compact_output_bytes": outputStats.Bytes,
		"compact_stage": strings.TrimSpace(progress.Stage), "compact_active_merges": progress.ActiveMerges, "compact_merge_progress": progress.MergeProgress,
	})
	if err != nil {
		return err
	}
	for _, part := range batch.Parts {
		if _, err := s.updateCompactProgress(ctx, part, workerID, patch, now); err != nil {
			return fmt.Errorf("update compact progress for %s/%s: %w", part.JobID, part.PartID, err)
		}
	}

	return nil
}

func (s *Store) ReleaseStaleCompactingParts(ctx context.Context, now time.Time, staleAfter time.Duration) (int, error) {
	if staleAfter <= 0 {
		return 0, fmt.Errorf("compact stale timeout must be greater than zero, got %s", staleAfter)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	n, err := s.releaseStaleCompactingPartsTx(ctx, tx, now, staleAfter)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
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

	if err := s.insertPartTx(ctx, tx, output); err != nil {
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

func compactReadyAtForRelease(part Part, now time.Time) string {
	for _, value := range []string{part.CompactReadyAt, part.ProgressUpdatedAt, part.UpdatedAt, part.CompactingAt} {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return formatTime(now)
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
	patch := map[string]any{}
	patch["updated_at"] = formatTime(now)
	patch["progress_updated_at"] = formatTime(now)
	if progress.QueryProgress != nil {
		patch["read_rows"] = progress.QueryProgress.ReadRows
		patch["read_bytes"] = progress.QueryProgress.ReadBytes
		patch["total_rows_approx"] = progress.QueryProgress.TotalRowsApprox
		patch["written_rows"] = progress.QueryProgress.WrittenRows
		patch["written_bytes"] = progress.QueryProgress.WrittenBytes
	}
	if progress.SourceActivePartStats != nil {
		patch["source_active_part_count"] = progress.SourceActivePartStats.Count
		patch["source_active_part_rows"] = progress.SourceActivePartStats.Rows
		patch["source_active_part_bytes"] = progress.SourceActivePartStats.Bytes
	}
	if progress.DestinationActivePartStats != nil {
		patch["destination_active_part_count"] = progress.DestinationActivePartStats.Count
		patch["destination_active_part_rows"] = progress.DestinationActivePartStats.Rows
		patch["destination_active_part_bytes"] = progress.DestinationActivePartStats.Bytes
	}
	if progress.DestinationFailedMerges != nil {
		patch["destination_failed_merges"] = *progress.DestinationFailedMerges
	}
	if progress.StageProgress != nil {
		patch["rewrite_stage"] = progress.StageProgress.Stage
		patch["rewrite_stage_started_at"] = formatTime(progress.StageProgress.StageStartedAt)
		patch["rewrite_stage_elapsed_ms"] = progress.StageProgress.StageElapsedMs
		patch["rewrite_total_elapsed_ms"] = progress.StageProgress.TotalElapsedMs
		patch["rewrite_stage_durations_ms"] = progress.StageProgress.CompletedStageDurationsMs
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	updates := `updated_at = $1, data = data || $2::jsonb`
	args := []any{formatTime(now), data, jobID, partID, workerID}
	if stats := progress.DestinationActivePartStats; stats != nil {
		updates += `, compact_bytes = $6, compact_eligible = $7::boolean AND
 COALESCE(btrim(data->>'destination_database'), '') <> '' AND
 COALESCE(btrim(data->>'destination_table'), '') <> '' AND
 COALESCE(btrim(data->>'destination_schema'), '') <> '' AND
 EXISTS (SELECT FROM jsonb_each_text(COALESCE(NULLIF(data->'destination_active_partition_counts', 'null'::jsonb), '{}'::jsonb)) p WHERE btrim(p.key) <> '' AND p.value::numeric > 1),
 compact_normalized = $8::boolean AND
 (SELECT count(*) = 1 AND COALESCE(bool_and(p.value::numeric = 1), false)
 FROM jsonb_each_text(COALESCE(NULLIF(data->'destination_active_partition_counts', 'null'::jsonb), '{}'::jsonb)) p WHERE btrim(p.key) <> '' AND p.value::numeric > 0)`
		args = append(args, pgtype.Numeric{Int: new(big.Int).SetUint64(stats.Bytes), Valid: true}, stats.Count > 0, stats.Count == 1)
	}
	tag, err := s.pool.Exec(ctx, `UPDATE `+s.tableSQL+` SET `+updates+` WHERE job_id = $3 AND part_id = $4 AND status = 'IN_PROGRESS' AND worker_id = $5`, args...)

	if err == nil && tag.RowsAffected() != 1 {
		err = &conditionalCheckFailedError{message: fmt.Sprintf("part %s/%s did not match expected state", jobID, partID)}
	}

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
	values := make([]string, 0, len(statuses))
	for _, status := range statuses {
		if strings.TrimSpace(string(status)) == "" {
			return nil, errors.New("status is required")
		}
		values = append(values, string(status))
	}
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT job_id FROM `+s.tableSQL+` WHERE status = ANY($1::text[]) ORDER BY job_id`, values)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *Store) ListJobsByStatus(ctx context.Context, statuses ...Status) ([]Job, error) {
	values := make([]string, 0, len(statuses))
	for _, status := range statuses {
		if strings.TrimSpace(string(status)) == "" {
			return nil, errors.New("status is required")
		}
		values = append(values, string(status))
	}
	rows, err := s.pool.Query(ctx, `WITH selected AS MATERIALIZED (
 SELECT job_id, status, created_at, updated_at, COALESCE(data->>'job_name', '') AS name,
 COALESCE((data->>'destination_active_part_count')::numeric, 0) AS part_count,
 COALESCE(NULLIF(data->'destination_active_partition_counts', 'null'::jsonb), '{}'::jsonb) AS partitions
 FROM `+s.tableSQL+` WHERE status = ANY($1::text[])
 ), partition_counts AS (
 SELECT job_id, count(DISTINCT p.key) AS count FROM selected,
 LATERAL jsonb_each_text(partitions) p
 WHERE status <> 'SUPERSEDED' AND btrim(p.key) <> '' AND p.value::numeric > 0 GROUP BY job_id
 ) SELECT s.job_id, s.status, count(*), COALESCE(min(NULLIF(s.name, '')), ''), max(s.name),
 min(s.created_at), max(s.updated_at), sum(CASE WHEN s.status <> 'SUPERSEDED' THEN part_count ELSE 0 END)::text,
 COALESCE(max(p.count), 0)
 FROM selected s LEFT JOIN partition_counts p USING (job_id)
 GROUP BY s.job_id, s.status ORDER BY s.job_id, s.status`, values)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		var jobID, minName, maxName, createdAt, updatedAt, activeCount string
		var status Status
		var count, partitions int
		if err := rows.Scan(&jobID, &status, &count, &minName, &maxName, &createdAt, &updatedAt, &activeCount, &partitions); err != nil {
			return nil, err
		}
		if minName != maxName {
			return nil, fmt.Errorf("job %s has conflicting job_name values %q and %q", jobID, minName, maxName)
		}
		if len(jobs) == 0 || jobs[len(jobs)-1].JobID != jobID {
			jobs = append(jobs, Job{JobID: jobID, Counts: map[Status]int{}, SubmittedAt: createdAt})
		}
		job := &jobs[len(jobs)-1]
		if job.Name != "" && minName != "" && job.Name != minName {
			return nil, fmt.Errorf("job %s has conflicting job_name values %q and %q", jobID, job.Name, minName)
		}
		if minName != "" {
			job.Name = minName
		}
		n, err := strconv.ParseUint(activeCount, 10, 64)
		if err != nil {
			return nil, err
		}
		job.Total += count
		job.Counts[status] = count
		job.DestinationActivePartCount += n
		job.DestinationPartitionCount = partitions
		if createdAt < job.SubmittedAt {
			job.SubmittedAt = createdAt
		}
		if updatedAt > job.UpdatedAt {
			job.UpdatedAt = updatedAt
		}
	}
	return jobs, rows.Err()
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
