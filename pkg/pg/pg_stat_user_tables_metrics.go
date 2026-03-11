package pg

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const PGStatUserTablesQuery = `
SELECT JSON_OBJECT_AGG(
	CONCAT(relname, '_', relid),
	JSON_BUILD_OBJECT(
        'last_autovacuum',last_autovacuum,
        'last_autoanalyze',last_autoanalyze,
        'autovacuum_count',autovacuum_count,
        'autoanalyze_count',autoanalyze_count,
        'n_live_tup',n_live_tup,
        'n_dead_tup',n_dead_tup,
        'n_mod_since_analyze',n_mod_since_analyze,
        'n_ins_since_vacuum',n_ins_since_vacuum,
        'seq_scan',seq_scan,
        'seq_tup_read',seq_tup_read,
        'idx_scan',idx_scan,
        'idx_tup_fetch',idx_tup_fetch
    )
)
as stats
FROM pg_stat_user_tables
`

// PGStatUserTables collects statistics from pg_stat_user_tables, computes deltas for various metrics per table,
// and emits them as metrics for monitoring purposes. It tracks metrics such as autovacuum counts, live/dead tuples,
// and sequential/index scans, providing insights into table activity and performance over time.
func PGStatUserTables(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var jsonResult string
		err := utils.QueryRowWithPrefix(pgPool, ctx, PGStatUserTablesQuery).Scan(&jsonResult)
		if err != nil {
			if err.Error() == "can't scan into dest[0]: cannot scan NULL into *string" {
				return errors.New("pg_stat_user_tables returned no data, likely this means there are no user tables in the database")
			}
			return err
		}

		var tableStats map[string]utils.PGUserTables
		err = json.Unmarshal([]byte(jsonResult), &tableStats)
		if err != nil {
			return err
		}

		// tableStats should now be a mapping from strings ('{name}_{id}') to values
		// some of those values are cumulative other are not.

		if state.Cache.PGUserTables != nil {
			// Prepare maps for each metric
			lastAutoVacuumMap := make(map[string]time.Time)
			lastAutoAnalyzeMap := make(map[string]time.Time)
			autoVacuumCountMap := make(map[string]int64)
			autoAnalyzeCountMap := make(map[string]int64)
			liveTuplesMap := make(map[string]int64)
			deadTuplesMap := make(map[string]int64)
			nModSinceAnalyzeMap := make(map[string]int64)
			nInsSinceVacuumMap := make(map[string]int64)
			seqScansMap := make(map[string]int64)
			seqTuplesReadMap := make(map[string]int64)
			idxScansMap := make(map[string]int64)
			idxTuplesFetchMap := make(map[string]int64)

			for tableKey, currentStats := range tableStats {
				if previousStats, exists := state.Cache.PGUserTables[tableKey]; exists {
					autoVacuumCountDelta := currentStats.AutoVacuumCount - previousStats.AutoVacuumCount
					autoAnalyzeCountDelta := currentStats.AutoAnalyzeCount - previousStats.AutoAnalyzeCount
					seqScanDelta := currentStats.SeqScan - previousStats.SeqScan
					seqTupReadDelta := currentStats.SeqTupRead - previousStats.SeqTupRead
					idxScanDelta := currentStats.IdxScan - previousStats.IdxScan
					idxTupFetchDelta := currentStats.IdxTupFetch - previousStats.IdxTupFetch

					if !currentStats.LastAutoVacuum.IsZero() {
						lastAutoVacuumMap[tableKey] = currentStats.LastAutoVacuum
					}
					if !currentStats.LastAutoAnalyze.IsZero() {
						lastAutoAnalyzeMap[tableKey] = currentStats.LastAutoAnalyze
					}

					if autoVacuumCountDelta >= 0 {
						autoVacuumCountMap[tableKey] = autoVacuumCountDelta
					}
					if autoAnalyzeCountDelta >= 0 {
						autoAnalyzeCountMap[tableKey] = autoAnalyzeCountDelta
					}
					liveTuplesMap[tableKey] = currentStats.NLiveTup
					deadTuplesMap[tableKey] = currentStats.NDeadTup
					nModSinceAnalyzeMap[tableKey] = currentStats.NModSinceAnalyze
					nInsSinceVacuumMap[tableKey] = currentStats.NInsSinceVacuum
					if seqScanDelta >= 0 {
						seqScansMap[tableKey] = seqScanDelta
					}
					if seqTupReadDelta >= 0 {
						seqTuplesReadMap[tableKey] = seqTupReadDelta
					}
					if idxScanDelta >= 0 {
						idxScansMap[tableKey] = idxScanDelta
					}
					if idxTupFetchDelta >= 0 {
						idxTuplesFetchMap[tableKey] = idxTupFetchDelta
					}
				}
			}

			metricsToEmit := []struct {
				metric metrics.MetricDef
				value  any
			}{
				{metrics.PGLastAutoVacuum, lastAutoVacuumMap},
				{metrics.PGLastAutoAnalyze, lastAutoAnalyzeMap},
				{metrics.PGAutoVacuumCountM, autoVacuumCountMap},
				{metrics.PGAutoAnalyzeCount, autoAnalyzeCountMap},
				{metrics.PGNLiveTuples, liveTuplesMap},
				{metrics.PGNDeadTuples, deadTuplesMap},
				{metrics.PGNModSinceAnalyze, nModSinceAnalyzeMap},
				{metrics.PGNInsSinceVacuum, nInsSinceVacuumMap},
				{metrics.PGSeqScan, seqScansMap},
				{metrics.PGSeqTupRead, seqTuplesReadMap},
				{metrics.PGIdxScan, idxScansMap},
				{metrics.PGIdxTupFetch, idxTuplesFetchMap},
			}

			for _, m := range metricsToEmit {
				err = EmitMetric(state, m.metric, m.value)
				if err != nil {
					return err
				}
			}
		}
		state.Cache.PGUserTables = tableStats
		return nil
	}
}
