package rewrite

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/metrics"
)

const compactProgressPollInterval = 5 * time.Second

type clickHouseMerge struct {
	PartitionID                string   `json:"partition_id"`
	ResultPartName             string   `json:"result_part_name"`
	Elapsed                    float64  `json:"elapsed"`
	Progress                   float64  `json:"progress"`
	NumParts                   uint64   `json:"num_parts"`
	SourcePartNames            []string `json:"source_part_names"`
	RowsRead                   uint64   `json:"rows_read"`
	BytesReadUncompressed      uint64   `json:"bytes_read_uncompressed"`
	TotalSizeBytesUncompressed uint64   `json:"total_size_bytes_uncompressed"`
}

func (c Compactor) observeCompactProgress(ctx context.Context, p Processor, item CompactWorkItem, target mergeWaitTarget, inputStats metrics.PartStats) error {
	lastStateReport := time.Time{}
	mergeMaxBlockSizeBytes := p.MergeTreeSettings.MergeMaxBlockSizeBytes
	var handledMemoryLimitFailures uint64
	for {
		failedMerges, err := p.destinationFailedMergeSummary(ctx, target)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if clickHouseMemoryLimitError(err) {
				if sleepOrDone(ctx, compactProgressPollInterval) != nil {
					return nil
				}
				continue
			}
			return fmt.Errorf("observe compact merge failures: %w", err)
		}
		if failedMerges.MemoryLimitCount > handledMemoryLimitFailures {
			failures := failedMerges.MemoryLimitCount - handledMemoryLimitFailures
			next := mergeMaxBlockSizeBytes
			for range failures {
				next = max(next/2, minAdaptiveMergeMaxBlockSizeBytes)
			}
			if next >= mergeMaxBlockSizeBytes {
				return fmt.Errorf("destination merge exceeded memory limit with merge_max_block_size_bytes already at minimum %d: %s", mergeMaxBlockSizeBytes, failedMerges.LatestException)
			}
			if err := recoverCompactMergeAfterMemoryFailure(ctx, p, target, next); err != nil {
				return err
			}
			slog.Warn(
				"reduced merge block byte limit after memory-limit merge failure",
				"stage", "adjusting_merge_memory",
				"job_id", item.JobID,
				"part_id", item.OutputPartID,
				"destination_table", target.tableSQL(),
				"memory_limit_failures", failedMerges.MemoryLimitCount,
				"previous_merge_max_block_size_bytes", mergeMaxBlockSizeBytes,
				"merge_max_block_size_bytes", next,
			)
			mergeMaxBlockSizeBytes = next
			handledMemoryLimitFailures = failedMerges.MemoryLimitCount
		}
		nonMemoryFailures := failedMerges.Count - failedMerges.MemoryLimitCount
		if nonMemoryFailures >= maxCompactMergeFailures {
			return fmt.Errorf("destination merges failed %d non-memory times (limit %d): %s", nonMemoryFailures, maxCompactMergeFailures, failedMerges.LatestException)
		}
		partitions, err := p.activePartPartitionStats(ctx, target.Database, target.Table)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if clickHouseMemoryLimitError(err) {
				if sleepOrDone(ctx, compactProgressPollInterval) != nil {
					return nil
				}
				continue
			}
			return fmt.Errorf("observe compact partition progress: %w", err)
		}
		merges, err := p.compactMerges(ctx, target)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if clickHouseMemoryLimitError(err) {
				if sleepOrDone(ctx, compactProgressPollInterval) != nil {
					return nil
				}
				continue
			}
			return fmt.Errorf("observe compact merge progress: %w", err)
		}
		stage := "waiting_for_merge_selection"
		if len(merges) > 0 {
			stage = "merging"
		}
		stats := summarizePartPartitions(partitions)
		c.metrics().ObserveCompactProgress(item.JobID, item.OutputPartID, stage, stats, compactPartitionMetrics(partitions), merges)
		now := time.Now()
		if c.ProgressInterval > 0 && (lastStateReport.IsZero() || now.Sub(lastStateReport) >= c.ProgressInterval) {
			activeMerges, mergeProgress := compactMergeSummary(merges)
			if err := c.reportProgress(ctx, item, CompactProgressSnapshot{
				InputStats:       inputStats,
				DestinationStats: stats,
				Stage:            stage,
				ActiveMerges:     activeMerges,
				MergeProgress:    mergeProgress,
			}); err != nil {
				return fmt.Errorf("report compact merge progress: %w", err)
			}
			lastStateReport = now
		}

		timer := time.NewTimer(compactProgressPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func recoverCompactMergeAfterMemoryFailure(ctx context.Context, p Processor, target mergeWaitTarget, mergeMaxBlockSizeBytes uint64) error {
	table := target.tableSQL()
	if err := p.ClickHouse.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
		return fmt.Errorf("stop destination merges after memory-limit failure: %w", err)
	}
	query := "ALTER TABLE " + table + " MODIFY SETTING merge_max_block_size_bytes = " + strconv.FormatUint(mergeMaxBlockSizeBytes, 10)
	if err := p.ClickHouse.Exec(ctx, query); err != nil {
		startErr := p.ClickHouse.Exec(ctx, "SYSTEM START MERGES "+table)
		if startErr != nil {
			return fmt.Errorf("reduce merge_max_block_size_bytes after memory-limit merge failure: %w; additionally failed to restart merges: %v", err, startErr)
		}
		return fmt.Errorf("reduce merge_max_block_size_bytes after memory-limit merge failure: %w", err)
	}
	if err := p.ClickHouse.Exec(ctx, "SYSTEM START MERGES "+table); err != nil {
		return fmt.Errorf("restart destination merges after memory-limit failure: %w", err)
	}
	partitions, err := p.activePartPartitionStats(ctx, target.Database, target.Table)
	if err != nil {
		return fmt.Errorf("find destination partition to optimize after memory-limit failure: %w", err)
	}
	for _, partition := range partitions {
		if partition.Parts > 1 {
			runOptimizeAsync(ctx, p.ClickHouse, compactOptimizeQuery(target, partition), chhttp.QueryOptions{})
			break
		}
	}
	return nil
}

func (p Processor) compactMerges(ctx context.Context, target mergeWaitTarget) ([]metrics.CompactMerge, error) {
	query := "SELECT partition_id, result_part_name, elapsed, progress, num_parts, source_part_names, rows_read, bytes_read_uncompressed, total_size_bytes_uncompressed FROM system.merges WHERE database = " +
		chhttp.StringLiteral(target.Database) + " AND table = " + chhttp.StringLiteral(target.Table) +
		" ORDER BY partition_id, result_part_name FORMAT JSONEachRow"
	out, err := p.ClickHouse.QueryStringWithOptions(ctx, query, chhttp.QueryOptions{Settings: chhttp.QuerySettings{
		"output_format_json_quote_64bit_integers": "0",
	}})
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(out))
	var rows []clickHouseMerge
	for {
		var row clickHouseMerge
		if err := decoder.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("decode system.merges row: %w", err)
		}
		if row.Progress < 0 {
			return nil, fmt.Errorf("ClickHouse merge progress must be non-negative, got %f", row.Progress)
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	partRows, err := p.compactSourcePartRows(ctx, target, rows)
	if err != nil {
		return nil, err
	}
	merges := make([]metrics.CompactMerge, 0, len(rows))
	for _, row := range rows {
		var totalRows uint64
		for _, partName := range row.SourcePartNames {
			totalRows += partRows[partName]
		}
		merges = append(merges, metrics.CompactMerge{
			PartitionID:    row.PartitionID,
			ResultPartName: row.ResultPartName,
			Elapsed:        time.Duration(row.Elapsed * float64(time.Second)),
			Progress:       row.Progress,
			SourceParts:    row.NumParts,
			RowsRead:       row.RowsRead,
			RowsTotal:      totalRows,
			BytesRead:      row.BytesReadUncompressed,
			BytesTotal:     row.TotalSizeBytesUncompressed,
		})
	}
	return merges, nil
}

func (p Processor) compactSourcePartRows(ctx context.Context, target mergeWaitTarget, merges []clickHouseMerge) (map[string]uint64, error) {
	names := map[string]struct{}{}
	for _, merge := range merges {
		for _, name := range merge.SourcePartNames {
			names[name] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	literals := make([]string, 0, len(ordered))
	for _, name := range ordered {
		literals = append(literals, chhttp.StringLiteral(name))
	}
	query := "SELECT name, rows FROM system.parts WHERE database = " + chhttp.StringLiteral(target.Database) +
		" AND table = " + chhttp.StringLiteral(target.Table) + " AND name IN (" + strings.Join(literals, ",") + ") FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := chhttp.FormatTSVStrings(out, 2)
	if err != nil {
		return nil, err
	}
	result := make(map[string]uint64, len(rows))
	for _, row := range rows {
		count, err := chhttp.ParseUInt(row[1])
		if err != nil {
			return nil, err
		}
		result[row[0]] = count
	}
	return result, nil
}

func compactMergeSummary(merges []metrics.CompactMerge) (uint64, float64) {
	if len(merges) == 0 {
		return 0, 0
	}
	var weightedProgress float64
	var totalBytes uint64
	for _, merge := range merges {
		weightedProgress += merge.Progress * float64(merge.BytesTotal)
		totalBytes += merge.BytesTotal
	}
	if totalBytes > 0 {
		return uint64(len(merges)), weightedProgress / float64(totalBytes)
	}
	var progress float64
	for _, merge := range merges {
		progress += merge.Progress
	}
	return uint64(len(merges)), progress / float64(len(merges))
}
