package rewrite

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/manifest"
	"github.com/PostHog/partforge/internal/metrics"
)

const DefaultInsertChunkMinRows uint64 = 10_000_000

// Keep completed work separate from the current attempt, which can be discarded.
type insertProgress struct {
	start, end, total uint64
	completed         metrics.QueryProgress
}

func (p insertProgress) snapshot(current metrics.QueryProgress, committed bool) ProgressSnapshot {
	if p.total == 0 {
		return ProgressSnapshot{QueryProgress: &current}
	}
	progress := p.completed
	progress.ReadRows += current.ReadRows
	progress.ReadBytes += current.ReadBytes
	progress.WrittenRows += current.WrittenRows
	progress.WrittenBytes += current.WrittenBytes
	fraction := float64(0)
	if current.TotalRowsApprox > 0 {
		fraction = min(1, float64(current.ReadRows)/float64(current.TotalRowsApprox))
		// Extrapolate the current range's read estimate over the remaining input.
		// Reads may exceed source rows, e.g. UNION ALL scans or granule boundaries.
		progress.TotalRowsApprox = p.completed.ReadRows + uint64(math.Ceil(
			float64(current.TotalRowsApprox)*float64(p.total-p.start)/float64(p.end-p.start)))
	}
	if committed {
		fraction = 1 // Also completes ranges whose SELECT reads or writes zero rows.
		if p.end == p.total {
			progress.TotalRowsApprox = progress.ReadRows
		}
	}
	percent := 100 * (float64(p.start) + float64(p.end-p.start)*fraction) / float64(p.total)
	return ProgressSnapshot{QueryProgress: &progress, InsertProgressPercent: &percent}
}

func insertChunkCount(rows, minRows uint64) uint64 {
	if minRows == 0 {
		return 1
	}
	return max(uint64(1), min(uint64(20), rows/minRows))
}

func insertChunkFilter(source manifest.TableRef, start, end uint64) string {
	return "{" + chhttp.StringLiteral(source.Database+"."+source.Table) + ": " +
		chhttp.StringLiteral(fmt.Sprintf("_part_offset >= %d AND _part_offset < %d", start, end)) + "}"
}

func completedInsertTable(m manifest.Manifest) string {
	return chhttp.TableSQL(m.Dest.Database, m.Dest.Table+"__partforge_completed")
}

func (p Processor) prepareInsertChunks(ctx context.Context, m manifest.Manifest) error {
	// SQL-level settings override HTTP settings. Reserve this setting so a rewrite
	// cannot accidentally remove the range restriction, including in a subquery.
	if strings.Contains(strings.ToLower(m.SQL.InsertSelect), "additional_table_filters") {
		return fmt.Errorf("chunked insert reserves additional_table_filters; remove it from the SQL or use -insert-chunk-min-rows=0")
	}
	if _, ok := p.InsertSettings["additional_table_filters"]; ok {
		return fmt.Errorf("chunked insert reserves additional_table_filters")
	}
	stats, err := p.activePartStats(ctx, m.Source.Database, m.Source.Table)
	if err != nil {
		return fmt.Errorf("validate chunk source: %w", err)
	}
	if stats.Count != 1 {
		return fmt.Errorf("chunked insert requires exactly one immutable source part, got %d", stats.Count)
	}
	if err := p.ClickHouse.Exec(ctx, "CREATE TABLE "+completedInsertTable(m)+" AS "+chhttp.TableSQL(m.Dest.Database, m.Dest.Table)); err != nil {
		return fmt.Errorf("create completed insert table: %w", err)
	}
	return nil
}

func (p Processor) promoteInsertChunk(ctx context.Context, m manifest.Manifest) error {
	partitions, err := p.activePartPartitionStats(ctx, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return fmt.Errorf("list insert staging partitions: %w", err)
	}
	for _, partition := range partitions {
		query := "ALTER TABLE " + chhttp.TableSQL(m.Dest.Database, m.Dest.Table) +
			" MOVE PARTITION ID " + chhttp.StringLiteral(partition.PartitionID) + " TO TABLE " + completedInsertTable(m)
		if err := p.ClickHouse.Exec(ctx, query); err != nil {
			// Some partitions may already have moved. Never rerun the insert here.
			return fmt.Errorf("promote insert partition %s: %w", partition.PartitionID, err)
		}
	}
	return nil
}
