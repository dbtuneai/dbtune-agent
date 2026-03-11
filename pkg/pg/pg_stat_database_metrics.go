package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const PGStatDatabaseQuery = `
SELECT
    blks_read,
    blks_hit,
    tup_returned,
    tup_fetched,
    tup_inserted,
    tup_updated,
    tup_deleted,
    temp_files,
    temp_bytes,
    deadlocks,
    idle_in_transaction_time
FROM pg_stat_database
WHERE datname = current_database();
`

// PGStatDatabase collects statistics from pg_stat_database and computes deltas.
// It also calculates the pg_cache_hit_ratio from blks_read and blks_hit.
func PGStatDatabase(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var blksRead, blksHit, tupReturned, tupFetched, tupInserted, tupUpdated, tupDeleted, tempFiles, tempBytes, deadlocks int64
		var idleInTransactionTime float64
		err := utils.QueryRowWithPrefix(pgPool, ctx, PGStatDatabaseQuery).Scan(
			&blksRead,
			&blksHit,
			&tupReturned,
			&tupFetched,
			&tupInserted,
			&tupUpdated,
			&tupDeleted,
			&tempFiles,
			&tempBytes,
			&deadlocks,
			&idleInTransactionTime,
		)

		if err != nil {
			return err
		}

		// Check if we have cached values from the previous collection
		if state.Cache.PGDatabase.BlksHit > 0 && state.Cache.PGDatabase.BlksRead > 0 {
			// Calculate deltas
			blksHitDelta := blksHit - state.Cache.PGDatabase.BlksHit
			blksReadDelta := blksRead - state.Cache.PGDatabase.BlksRead

			// Handle counter resets (e.g., after PostgreSQL crash or immediate shutdown)
			if blksHitDelta >= 0 && blksReadDelta >= 0 {
				// Calculate cache hit ratio for this interval
				var bufferCacheHitRatio float64
				totalBlocks := blksHitDelta + blksReadDelta
				if totalBlocks > 0 {
					bufferCacheHitRatio = float64(blksHitDelta) / float64(totalBlocks) * 100.0
				} else {
					// No block activity in this interval
					bufferCacheHitRatio = 0.0
				}

				// Validate the calculated ratio is within reasonable bounds
				if bufferCacheHitRatio < 0.0 || bufferCacheHitRatio > 100.0 {
					return fmt.Errorf("calculated cache hit ratio is out of bounds: %.2f%%", bufferCacheHitRatio)
				}
				CHRmetricEntry, err := metrics.PGCacheHitRatio.AsFlatValue(bufferCacheHitRatio)
				if err != nil {
					return err
				}
				state.AddMetric(CHRmetricEntry)
			}
		}

		mappingsInt := []MetricMapping[int64]{
			{Current: &tupReturned, Previous: &state.Cache.PGDatabase.TuplesReturned, Metric: metrics.PGTuplesReturned},
			{Current: &tupFetched, Previous: &state.Cache.PGDatabase.TuplesFetched, Metric: metrics.PGTuplesFetched},
			{Current: &tupInserted, Previous: &state.Cache.PGDatabase.TuplesInserted, Metric: metrics.PGTuplesInserted},
			{Current: &tupUpdated, Previous: &state.Cache.PGDatabase.TuplesUpdated, Metric: metrics.PGTuplesUpdated},
			{Current: &tupDeleted, Previous: &state.Cache.PGDatabase.TuplesDeleted, Metric: metrics.PGTuplesDeleted},
			{Current: &tempFiles, Previous: &state.Cache.PGDatabase.TempFiles, Metric: metrics.PGTempFiles},
			{Current: &tempBytes, Previous: &state.Cache.PGDatabase.TempBytes, Metric: metrics.PGTempBytes},
			{Current: &deadlocks, Previous: &state.Cache.PGDatabase.Deadlocks, Metric: metrics.PGDeadlocks},
		}

		mappingsFloat := []MetricMapping[float64]{
			{Current: &idleInTransactionTime, Previous: &state.Cache.PGDatabase.IdleInTransactionTime, Metric: metrics.PGIdleInTransactionTime},
		}

		if err := EmitCumulativeMetricsMap(state, mappingsInt); err != nil {
			return err
		}
		if err := EmitCumulativeMetricsMap(state, mappingsFloat); err != nil {
			return err
		}

		// Update cache for next iteration
		state.Cache.PGDatabase = agent.PGDatabase{
			BlksRead:              blksRead,
			BlksHit:               blksHit,
			TuplesReturned:        tupReturned,
			TuplesFetched:         tupFetched,
			TuplesInserted:        tupInserted,
			TuplesUpdated:         tupUpdated,
			TuplesDeleted:         tupDeleted,
			TempFiles:             tempFiles,
			TempBytes:             tempBytes,
			Deadlocks:             deadlocks,
			IdleInTransactionTime: idleInTransactionTime,
			Timestamp:             time.Now(),
		}

		return nil
	}
}
