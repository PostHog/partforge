package state

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestSelectCompactBatchPartsAllowsSingleMultiPartArtifact(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-1",
			DestinationActivePartCount: 4,
			DestinationActivePartBytes: 1024,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 4,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxBytes: 2048})

	if len(selected) != 1 || selected[0].PartID != "part-1" {
		t.Fatalf("selected = %+v, want part-1", selected)
	}
}

func TestSelectCompactBatchPartsAllowsOversizedSingleMultiPartArtifact(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-1",
			DestinationActivePartCount: 4,
			DestinationActivePartBytes: 4096,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 4,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxBytes: 2048})

	if len(selected) != 1 || selected[0].PartID != "part-1" {
		t.Fatalf("selected = %+v, want oversized part-1", selected)
	}
}

func TestSelectCompactBatchPartsNormalizesFragmentedArtifactAlone(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "fragmented",
			DestinationActivePartCount: 3,
			DestinationActivePartBytes: 300,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 3,
			},
		},
		{
			PartID:                     "normalized",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 8})

	if len(selected) != 1 || selected[0].PartID != "fragmented" {
		t.Fatalf("selected = %+v, want fragmented artifact alone", selected)
	}
}

func TestSelectCompactBatchPartsNormalizesIdlePartitionFirst(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{
		parts: []Part{
			{
				PartID:                     "busy",
				DestinationActivePartCount: 2,
				DestinationActivePartitionCounts: map[string]uint64{
					"busy": 2,
				},
			},
			{
				PartID:                     "idle",
				DestinationActivePartCount: 2,
				DestinationActivePartitionCounts: map[string]uint64{
					"idle": 2,
				},
			},
		},
		compactingPartitionIDs: []string{"busy"},
	}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 8})

	if len(selected) != 1 || selected[0].PartID != "idle" {
		t.Fatalf("selected = %+v, want idle fragmented artifact alone", selected)
	}
}

func TestSelectCompactBatchPartsDoesNotCombineFragmentedBusyPartitionThroughIdleOverlap(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{
		parts: []Part{
			{
				PartID:                     "fragmented",
				DestinationActivePartCount: 3,
				DestinationActivePartitionCounts: map[string]uint64{
					"busy": 2,
					"idle": 1,
				},
			},
			{
				PartID:                     "normalized",
				DestinationActivePartCount: 1,
				DestinationActivePartitionCounts: map[string]uint64{
					"idle": 1,
				},
			},
		},
		compactingPartitionIDs: []string{"busy"},
	}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 8})

	if len(selected) != 1 || selected[0].PartID != "fragmented" {
		t.Fatalf("selected = %+v, want fragmented artifact alone", selected)
	}
}

func TestSelectCompactBatchPartsIgnoresCooldownField(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected := selectCompactBatchParts(compactGroup{
		parts: []Part{
			{
				PartID:                     "part-cooldown",
				DestinationActivePartCount: 2,
				DestinationActivePartBytes: 1024,
				DestinationActivePartitionCounts: map[string]uint64{
					"202606": 2,
				},
				CompactCooldownUntil: formatTime(now.Add(time.Hour)),
			},
		},
	}, CompactClaimOptions{MinInputParts: 2})
	if len(selected) != 1 || selected[0].PartID != "part-cooldown" {
		t.Fatalf("selected = %+v, want cooldown field ignored", selected)
	}
}

func TestSelectCompactBatchPartsIncludesRowsWithCooldownField(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected := selectCompactBatchParts(compactGroup{
		parts: []Part{
			{
				PartID:                     "part-fresh",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"202606": 1,
				},
			},
			{
				PartID:                     "part-cooldown",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"202606": 1,
				},
				CompactCooldownUntil: formatTime(now.Add(time.Hour)),
			},
		},
	}, CompactClaimOptions{MinInputParts: 2})

	if len(selected) != 2 || selected[0].PartID != "part-fresh" || selected[1].PartID != "part-cooldown" {
		t.Fatalf("selected = %+v, want fresh part with cooldown companion", selected)
	}
}

func TestCompactCandidateGroupsIncludesRowsWithCooldownField(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	groups := compactCandidateGroups([]Part{
		{
			JobID:                      "job-1",
			PartID:                     "part-cooldown",
			Bucket:                     "bucket",
			DestinationDatabase:        "db",
			DestinationTable:           "table",
			DestinationSchema:          "schema",
			DestinationActivePartCount: 2,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 2,
			},
			CompactCooldownUntil: formatTime(now.Add(time.Hour)),
		},
		{
			JobID:                      "job-1",
			PartID:                     "part-ready",
			Bucket:                     "bucket",
			DestinationDatabase:        "db",
			DestinationTable:           "table",
			DestinationSchema:          "schema",
			DestinationActivePartCount: 2,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 2,
			},
		},
	}, nil, CompactClaimOptions{})
	if len(groups) != 1 || len(groups[0].parts) != 2 || groups[0].parts[0].PartID != "part-cooldown" || groups[0].parts[1].PartID != "part-ready" {
		t.Fatalf("groups = %+v, want cooldown and ready parts", groups)
	}
}

func TestCompactCandidateGroupsSkipsExcludedJobs(t *testing.T) {
	groups := compactCandidateGroups([]Part{
		{
			JobID:                      "job-1",
			PartID:                     "part-1",
			Bucket:                     "bucket",
			DestinationDatabase:        "db",
			DestinationTable:           "table",
			DestinationSchema:          "schema",
			DestinationActivePartCount: 2,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 2,
			},
		},
		{
			JobID:                      "job-2",
			PartID:                     "part-2",
			Bucket:                     "bucket",
			DestinationDatabase:        "db",
			DestinationTable:           "table",
			DestinationSchema:          "schema",
			DestinationActivePartCount: 2,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 2,
			},
		},
	}, nil, CompactClaimOptions{ExcludedJobIDs: map[string]struct{}{"job-1": {}}})
	if len(groups) != 1 || len(groups[0].parts) != 1 || groups[0].parts[0].JobID != "job-2" {
		t.Fatalf("groups = %+v, want only non-excluded job-2", groups)
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

func TestCompactCandidateGroupsSeparateJobs(t *testing.T) {
	candidates := []Part{
		compactBatchTestPart("job-a", "part-a1", StatusCompactReady),
		compactBatchTestPart("job-b", "part-b1", StatusCompactReady),
		compactBatchTestPart("job-a", "part-a2", StatusCompactReady),
		compactBatchTestPart("job-b", "part-b2", StatusCompactReady),
	}
	compacting := []Part{
		compactBatchTestPart("job-a", "part-a-busy", StatusCompacting),
	}

	groups := compactCandidateGroups(candidates, compacting, CompactClaimOptions{})
	if len(groups) != 2 {
		t.Fatalf("groups = %+v, want one group per job", groups)
	}
	for _, group := range groups {
		if len(group.parts) != 2 {
			t.Fatalf("group = %+v, want two parts for one job", group)
		}
		jobID := group.parts[0].JobID
		selected := selectCompactBatchParts(group, CompactClaimOptions{MinInputParts: 2})
		if len(selected) != 2 {
			t.Fatalf("selected = %+v, want two parts for job %s", selected, jobID)
		}
		for _, part := range selected {
			if part.JobID != jobID {
				t.Fatalf("selected = %+v, mixed job %s into %s group", selected, part.JobID, jobID)
			}
		}
		switch jobID {
		case "job-a":
			if len(group.compactingPartitionIDs) != 1 || group.compactingPartitionIDs[0] != "partition-a" {
				t.Fatalf("job-a compacting partitions = %v, want partition-a", group.compactingPartitionIDs)
			}
		case "job-b":
			if len(group.compactingPartitionIDs) != 0 {
				t.Fatalf("job-b compacting partitions = %v, want none", group.compactingPartitionIDs)
			}
		default:
			t.Fatalf("unexpected job group %s", jobID)
		}
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

func TestCompactHeartbeatTimeUsesUpdatedAt(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	part := Part{
		JobID:        "job-1",
		PartID:       "part-1",
		UpdatedAt:    formatTime(now),
		CompactingAt: formatTime(now.Add(-time.Hour)),
	}
	got, err := compactHeartbeatTime(part)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(now) {
		t.Fatalf("compactHeartbeatTime = %s, want %s", got, now)
	}
}

func TestCompactStaleTimeUsesOldestLeaseTimestamp(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	part := Part{
		JobID:        "job-1",
		PartID:       "part-1",
		UpdatedAt:    formatTime(now),
		CompactingAt: formatTime(now.Add(-2 * time.Hour)),
	}
	got, err := compactStaleTime(part)
	if err != nil {
		t.Fatal(err)
	}
	want := now.Add(-2 * time.Hour)
	if !got.Equal(want) {
		t.Fatalf("compactStaleTime = %s, want %s", got, want)
	}
}

func TestSelectCompactBatchPartsDoesNotCombineInsufficientPartitions(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2})

	if len(selected) != 0 {
		t.Fatalf("selected = %+v, want no cross-partition batch", selected)
	}
}

func TestSelectCompactBatchPartsFillsMultipleIdlePartitions(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-a2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
		{
			PartID:                     "part-b2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 8})

	if len(selected) != 4 ||
		selected[0].PartID != "part-a1" ||
		selected[1].PartID != "part-a2" ||
		selected[2].PartID != "part-b1" ||
		selected[3].PartID != "part-b2" {
		t.Fatalf("selected = %+v, want both eligible partitions", selected)
	}
}

func TestSelectCompactBatchPartsDoesNotPartiallyFillSecondPartitionAtMaxArtifacts(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-a2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
		{
			PartID:                     "part-b2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 3})

	if len(selected) != 2 || selected[0].PartID != "part-a1" || selected[1].PartID != "part-a2" {
		t.Fatalf("selected = %+v, want only complete partition-a batch", selected)
	}
}

func TestSelectCompactBatchPartsDoesNotPartiallyFillSecondPartitionAtMaxBytes(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-a2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
		{
			PartID:                     "part-b2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxBytes: 350})

	if len(selected) != 2 || selected[0].PartID != "part-a1" || selected[1].PartID != "part-a2" {
		t.Fatalf("selected = %+v, want only complete partition-a batch", selected)
	}
}

func TestSelectCompactBatchPartsUsesSharedPartition(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2})

	if len(selected) != 2 || selected[0].PartID != "part-a" || selected[1].PartID != "part-b" {
		t.Fatalf("selected = %+v, want part-a and part-b", selected)
	}
}

func TestSelectCompactBatchPartsFillsSharedPartitionBatch(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-c",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 8})

	if len(selected) != 3 || selected[0].PartID != "part-a" || selected[1].PartID != "part-b" || selected[2].PartID != "part-c" {
		t.Fatalf("selected = %+v, want all compatible partition-a parts", selected)
	}
}

func TestSelectCompactBatchPartsStopsAtMaxArtifactsWhileFilling(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-c",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 2})

	if len(selected) != 2 || selected[0].PartID != "part-a" || selected[1].PartID != "part-b" {
		t.Fatalf("selected = %+v, want max-artifacts-limited part-a and part-b", selected)
	}
}

func TestSelectCompactBatchPartsHonorsRequiredPartitions(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 1, RequiredPartitionIDs: []string{"partition-b"}})

	if len(selected) != 1 || selected[0].PartID != "part-b" {
		t.Fatalf("selected = %+v, want only part-b", selected)
	}
}

func TestSelectCompactBatchPartsPrefersPartitionNotAlreadyCompacting(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{
		compactingPartitionIDs: []string{"partition-a"},
		parts: []Part{
			{
				PartID:                     "part-a1",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-a": 1,
				},
			},
			{
				PartID:                     "part-a2",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-a": 1,
				},
			},
			{
				PartID:                     "part-b1",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-b": 1,
				},
			},
			{
				PartID:                     "part-b2",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-b": 1,
				},
			},
		},
	}, CompactClaimOptions{MinInputParts: 2})

	if len(selected) != 2 || selected[0].PartID != "part-b1" || selected[1].PartID != "part-b2" {
		t.Fatalf("selected = %+v, want idle partition-b parts", selected)
	}
}

func TestSelectCompactBatchPartsFallsBackToAlreadyCompactingPartition(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{
		compactingPartitionIDs: []string{"partition-a"},
		parts: []Part{
			{
				PartID:                     "part-a1",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-a": 1,
				},
			},
			{
				PartID:                     "part-a2",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-a": 1,
				},
			},
		},
	}, CompactClaimOptions{MinInputParts: 2})

	if len(selected) != 2 || selected[0].PartID != "part-a1" || selected[1].PartID != "part-a2" {
		t.Fatalf("selected = %+v, want busy partition-a fallback", selected)
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
