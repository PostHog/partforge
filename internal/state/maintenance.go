package state

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// MaintainCompaction runs at most once per ten seconds across looping workers.
// A one-shot worker requests a pass immediately, still skipping active maintenance.
// The reservation and work commit together, so failure does not consume the run.
func (s *Store) MaintainCompaction(ctx context.Context, window, staleAfter time.Duration, now time.Time, once bool) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE `+s.relatedSQL("maintenance")+` SET next_run_at = $1
 WHERE id IN (SELECT id FROM `+s.relatedSQL("maintenance")+` WHERE next_run_at <= $2 OR $3::boolean FOR UPDATE SKIP LOCKED)`, now.Add(10*time.Second), now, once)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() == 0 {
		return 0, nil
	}
	if staleAfter > 0 {
		if _, err := s.releaseStaleCompactingPartsTx(ctx, tx, now, staleAfter); err != nil {
			return 0, err
		}
	}
	finalized, err := s.finalizeCompactReadyTx(ctx, tx, "", window, now)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return finalized, nil
}

func (s *Store) releaseStaleCompactingPartsTx(ctx context.Context, tx pgx.Tx, now time.Time, staleAfter time.Duration) (int, error) {
	rows, err := tx.Query(ctx, `SELECT data FROM `+s.tableSQL+` WHERE status = 'COMPACTING' AND compact_stale_at <= $1 FOR UPDATE SKIP LOCKED`, now.Add(-staleAfter))
	if err != nil {
		return 0, err
	}
	parts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Part, error) {
		var data []byte
		if err := row.Scan(&data); err != nil {
			return Part{}, err
		}
		return partFromJSON(data)
	})
	if err != nil {
		return 0, err
	}
	for _, part := range parts {
		if part.CompactReadyAt == "" {
			part.CompactReadyAt = compactReadyAtForRelease(part, now)
		}
		setStatus(&part, StatusCompactReady, now)
		part.WorkerID = ""
		part.CompactingAt = ""
		part.Error = ""
		part.CompactCooldownUntil = ""
		clearCompactProgress(&part)
		if err := s.savePartTx(ctx, tx, part); err != nil {
			return 0, err
		}
	}
	return len(parts), nil
}

func (s *Store) CompactDeadline(ctx context.Context, jobID string, window time.Duration) (time.Time, error) {
	if window <= 0 {
		return time.Time{}, nil
	}
	var readyAt time.Time
	err := s.pool.QueryRow(ctx, `SELECT original_compact_ready_at FROM `+s.tableSQL+` WHERE job_id = $1 AND original_compact_ready_at IS NOT NULL ORDER BY original_compact_ready_at DESC LIMIT 1`, jobID).Scan(&readyAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, errors.New("no original compact-ready timestamp found")
	}
	if err != nil {
		return time.Time{}, err
	}
	return readyAt.Add(window), nil
}

func (s *Store) FinalizeCompactReadyJob(ctx context.Context, jobID string, window time.Duration, now time.Time) (int, error) {
	if jobID == "" {
		return 0, errors.New("job id is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	n, err := s.finalizeCompactReadyTx(ctx, tx, jobID, window, now)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *Store) finalizeCompactReadyTx(ctx context.Context, tx pgx.Tx, jobID string, window time.Duration, now time.Time) (int, error) {
	type summary struct {
		jobID                         string
		readyAt                       *time.Time
		blocked, eligible, normalized bool
	}
	query := `SELECT job_id, max(original_compact_ready_at),
 bool_or(status IN ('READY', 'IN_PROGRESS', 'COMPACTING', 'FAILED')),
 bool_or(status = 'COMPACT_READY' AND compact_eligible),
 bool_or(status = 'COMPACT_READY' AND compact_normalized)
 FROM ` + s.tableSQL
	args := []any{}
	if jobID != "" {
		query += ` WHERE job_id = $1`
		args = append(args, jobID)
	}
	query += ` GROUP BY job_id HAVING bool_or(status = 'COMPACT_READY')`
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (summary, error) {
		var j summary
		err := row.Scan(&j.jobID, &j.readyAt, &j.blocked, &j.eligible, &j.normalized)
		return j, err
	})
	if err != nil {
		return 0, err
	}
	finalized := 0
	for _, j := range jobs {
		// A normalized artifact can finish even while other work remains active.
		normalizedOnly := j.normalized
		if !normalizedOnly {
			if j.blocked {
				continue
			}
			if window > 0 {
				if j.readyAt == nil {
					return 0, fmt.Errorf("job %s: no original compact-ready timestamp found", j.jobID)
				}
				if now.Before(j.readyAt.Add(window)) {
					continue
				}
			} else if j.eligible {
				continue
			} // Leave useful work for claimers when the window is disabled.
		}
		tag, err := tx.Exec(ctx, `WITH candidates AS (
 SELECT job_id, part_id FROM `+s.tableSQL+`
 WHERE job_id = $1 AND status = 'COMPACT_READY' AND (NOT $2::boolean OR compact_normalized)
 FOR UPDATE SKIP LOCKED)
 UPDATE `+s.tableSQL+` p SET status = 'FINISHED', updated_at = $3, compact_stale_at = NULL,
 data = (p.data - 'error' - 'compact_cooldown_until') || jsonb_build_object('status', 'FINISHED', 'updated_at', $3::text, 'finished_at', $3::text)
 FROM candidates c WHERE p.job_id = c.job_id AND p.part_id = c.part_id`, j.jobID, normalizedOnly, formatTime(now))
		if err != nil {
			return 0, err
		}
		finalized += int(tag.RowsAffected())
	}
	return finalized, nil
}
