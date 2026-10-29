package rewrite

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// pauseDestinationMerges makes the active part set stable until measurement and
// FREEZE finish. STOP MERGES prevents selection, but existing merges must drain.
func (p Processor) pauseDestinationMerges(ctx context.Context, target mergeWaitTarget) (func() error, error) {
	table := target.tableSQL()
	resume := func() error {
		// Cleanup must still work after the operation's context is cancelled.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := p.ClickHouse.Exec(cleanupCtx, "SYSTEM START MERGES "+table); err != nil {
			return fmt.Errorf("restore destination merges for %s: %w", table, err)
		}
		return nil
	}
	if err := p.ClickHouse.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
		return nil, errors.Join(fmt.Errorf("stop destination merges for snapshot: %w", err), resume())
	}
	// MergeTimeout is an optimization budget and may be deliberately tiny.
	// Snapshot correctness needs its own bounded wait, including after settling
	// timed out. The caller can still impose a shorter operation deadline.
	waitCtx, cancel := context.WithTimeout(ctx, DefaultMergeTimeout)
	defer cancel()
	interval := p.MergePollInterval
	if interval <= 0 {
		interval = defaultMergePollInterval
	}
	for {
		count, err := p.destinationMergeCount(waitCtx, target)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("drain destination merges for snapshot: %w", err), resume())
		}
		if count == 0 {
			return resume, nil
		}
		if err := sleepOrDone(waitCtx, interval); err != nil {
			return nil, errors.Join(fmt.Errorf("drain destination merges for snapshot: %w", err), resume())
		}
	}
}
