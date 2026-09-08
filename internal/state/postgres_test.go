package state

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMarkCompactPartFailed(t *testing.T) {
	now := time.Date(2026, 7, 21, 14, 30, 0, 0, time.UTC)
	part := Part{
		Status:                     StatusCompacting,
		WorkerID:                   "worker-1",
		CompactingAt:               formatTime(now.Add(-time.Minute)),
		CompactOutputPartID:        "compact-1",
		CompactProgressAt:          formatTime(now.Add(-time.Second)),
		CompactFinalizeRequestedAt: formatTime(now.Add(-time.Second)),
		CompactStage:               "merging",
		CompactActiveMerges:        2,
		CompactMergeProgress:       0.5,
	}
	cause := errors.New("optimize made no progress")

	markCompactPartFailed(&part, cause, now)

	if part.Status != StatusFailed || part.Error != cause.Error() || part.FailedAt != formatTime(now) {
		t.Fatalf("failed part = %+v", part)
	}
	if part.WorkerID != "" || part.CompactingAt != "" || part.CompactOutputPartID != "" || part.CompactStage != "" || part.CompactActiveMerges != 0 || part.CompactMergeProgress != 0 {
		t.Fatalf("failed part retained compact ownership or progress: %+v", part)
	}
}

func TestClearRewriteProgressClearsTotalRowsApprox(t *testing.T) {
	part := Part{ReadRows: 25, TotalRowsApprox: 100}
	clearRewriteProgress(&part)
	if part.ReadRows != 0 || part.TotalRowsApprox != 0 {
		t.Fatalf("part retained rewrite progress: %+v", part)
	}
}

func TestFailedRetryTarget(t *testing.T) {
	tests := []struct {
		name string
		part Part
		want Status
	}{
		{name: "rewrite", part: Part{}, want: StatusReady},
		{name: "compaction", part: Part{CompactReadyAt: "2026-07-21T14:00:00Z"}, want: StatusCompactReady},
		{name: "import", part: Part{CompactReadyAt: "2026-07-21T14:00:00Z", ImportingAt: "2026-07-21T15:00:00Z"}, want: StatusFinished},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := failedRetryTarget(test.part); got != test.want {
				t.Fatalf("failedRetryTarget() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestValidatePartRejectsPartialSourceRef(t *testing.T) {
	part := NewPart("job-1", "part-1", "bucket", "source/part-1", "finished/part-1", time.Now().UTC())
	part.SourceJobID = "job-source"

	err := validatePart(part)
	if err == nil {
		t.Fatal("expected partial source ref error")
	}
	if !strings.Contains(err.Error(), "source_job_id and source_part_id") {
		t.Fatalf("error = %v, want source ref error", err)
	}
}

func TestPartJSONPreservesSourceArtifactBytes(t *testing.T) {
	part := NewPart("job-1", "part-1", "bucket", "source/part-1", "finished/part-1", time.Now().UTC())
	part.SourceArtifactBytes = 1234
	data, err := partJSON(part)
	if err != nil {
		t.Fatal(err)
	}
	got, err := partFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceArtifactBytes != part.SourceArtifactBytes {
		t.Fatalf("source artifact bytes = %d, want %d", got.SourceArtifactBytes, part.SourceArtifactBytes)
	}
}

func TestValidatePartRejectsSelfSourceRef(t *testing.T) {
	part := NewPart("job-1", "part-1", "bucket", "source/part-1", "finished/part-1", time.Now().UTC())
	part.SourceJobID = part.JobID
	part.SourcePartID = part.PartID

	err := validatePart(part)
	if err == nil {
		t.Fatal("expected self source ref error")
	}
	if !strings.Contains(err.Error(), "cannot reference itself") {
		t.Fatalf("error = %v, want self source ref error", err)
	}
}

func TestCompactBatchFromPartsRejectsMixedJobs(t *testing.T) {
	_, err := compactBatchFromParts([]Part{
		compactBatchTestPart("job-a", "part-a", StatusCompacting),
		compactBatchTestPart("job-b", "part-b", StatusCompacting),
	})
	if err == nil {
		t.Fatal("expected mixed job compact batch error")
	}
	if !strings.Contains(err.Error(), "mixes job ids") {
		t.Fatalf("error = %v, want mixed job ids", err)
	}
}

func TestUpdateCompactProgressRejectsMixedJobBatch(t *testing.T) {
	err := (&Store{}).UpdateCompactProgress(context.Background(), CompactBatch{
		JobID: "job-a",
		Parts: []Part{
			compactBatchTestPart("job-a", "part-a", StatusCompacting),
			compactBatchTestPart("job-b", "part-b", StatusCompacting),
		},
	}, "compact-out", "worker", PartStats{}, PartStats{}, CompactProgress{}, time.Now().UTC())
	if err == nil {
		t.Fatal("expected mixed job compact batch error")
	}
	if !strings.Contains(err.Error(), "mixes job ids") {
		t.Fatalf("error = %v, want mixed job ids", err)
	}
}

func TestCompleteCompactionRejectsOutputFromDifferentJob(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	input := compactBatchTestPart("job-a", "part-a", StatusCompacting)
	output := NewCompactPart(
		"job-b",
		"compact-out",
		input.Bucket,
		"finished/compact-out",
		input.DestinationDatabase,
		input.DestinationTable,
		input.DestinationSchema,
		[]string{input.PartID},
		1,
		PartStats{Count: 1},
		map[string]uint64{"partition-a": 1},
		now,
		now,
	)

	err := (&Store{}).CompleteCompaction(context.Background(), CompactBatch{JobID: "job-a", Parts: []Part{input}}, output, "worker", now)
	if err == nil {
		t.Fatal("expected compact output job mismatch error")
	}
	if !strings.Contains(err.Error(), "does not match batch job id") {
		t.Fatalf("error = %v, want output job mismatch", err)
	}
}

func TestNewCompactPartSetsCompactReadyAt(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	readyAt := now.Add(-2 * time.Hour)
	part := NewCompactPart("job-1", "compact-1", "bucket", "finished/key", "db", "table", "schema", []string{"part-1"}, 1, PartStats{Count: 1}, map[string]uint64{"p": 1}, readyAt, now)
	if part.CreatedAt != formatTime(now) {
		t.Fatalf("created_at = %q, want %q", part.CreatedAt, formatTime(now))
	}
	if part.CompactReadyAt != formatTime(readyAt) {
		t.Fatalf("compact_ready_at = %q, want %q", part.CompactReadyAt, formatTime(readyAt))
	}
}

func TestCompactReadyAtForReleasePreservesStableTime(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	part := Part{
		CompactReadyAt:    formatTime(now.Add(-3 * time.Hour)),
		ProgressUpdatedAt: formatTime(now.Add(-2 * time.Hour)),
		UpdatedAt:         formatTime(now),
	}
	if got := compactReadyAtForRelease(part, now); got != part.CompactReadyAt {
		t.Fatalf("compactReadyAtForRelease = %q, want compact_ready_at %q", got, part.CompactReadyAt)
	}
}

func TestCompactReadyAtForReleaseBackfillsExistingRowsFromProgress(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	part := Part{
		ProgressUpdatedAt: formatTime(now.Add(-2 * time.Hour)),
		UpdatedAt:         formatTime(now),
	}
	if got := compactReadyAtForRelease(part, now); got != part.ProgressUpdatedAt {
		t.Fatalf("compactReadyAtForRelease = %q, want progress_updated_at %q", got, part.ProgressUpdatedAt)
	}
}

func compactBatchTestPart(jobID, partID string, status Status) Part {
	now := formatTime(time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC))
	return Part{
		JobID:                      jobID,
		PartID:                     partID,
		Status:                     status,
		Bucket:                     "bucket",
		SourceKey:                  "source/" + partID,
		FinishedKey:                "finished/" + partID,
		CreatedAt:                  now,
		UpdatedAt:                  now,
		DestinationDatabase:        "db",
		DestinationTable:           "table",
		DestinationSchema:          "schema",
		DestinationActivePartCount: 1,
		DestinationActivePartRows:  10,
		DestinationActivePartBytes: 100,
		DestinationActivePartitionCounts: map[string]uint64{
			"partition-a": 1,
		},
	}
}
