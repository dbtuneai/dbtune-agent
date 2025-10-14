package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Helper function reformat and emit a single metric
func EmitMetric(state *agent.MetricsState, metric metrics.MetricDef, value any) error {
	entry, err := metric.AsFlatValue(value)
	if err == nil {
		state.AddMetric(entry)
	} else {
		return err
	}
	return nil
}

type Number interface {
	~int64 | ~float64
}

type MetricMapping[T Number] struct {
	Current  *T
	Previous *T
	Metric   metrics.MetricDef
}

// Helper function to calculate the delta and emit a number of cumulative metrics
// The mapping takes the current value, the previous value and the metric to emit
// If either current or previous is nil, the metric is skipped
//
// # Note that cumulative metrics should be positive and monotonically increasing
//
// In some cases, the delta can be negative (e.g. if the counter has been reset)
// In those cases, the metric is also skipped
func EmitCumulativeMetricsMap[T Number](state *agent.MetricsState, mappings []MetricMapping[T]) error {
	for _, m := range mappings {
		if m.Previous != nil && m.Current != nil {
			delta := *m.Current - *m.Previous
			if *m.Previous >= 0 && *m.Current >= 0 && delta >= 0 {
				err := EmitMetric(state, m.Metric, delta)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

const PgStatStatementsQuery = `
/*dbtune*/
SELECT JSON_OBJECT_AGG(
	CONCAT(queryid, '_', userid, '_', dbid), 
	JSON_BUILD_OBJECT(
		'calls',calls,
		'total_exec_time',total_exec_time,
		'query_id', CONCAT(queryid, '_', userid, '_', dbid),
		'rows',rows
	)
)
AS qrt_stats
FROM (SELECT * FROM pg_stat_statements
	WHERE query !~ 'BEGIN|COMMIT|\.pg_\.|\.dbtune\.|^SELECT \$1$|\.version\.'
	ORDER BY calls DESC)
AS f
`

func PGStatStatements(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	// Get config when collecter creater
	pgConfig, _ := ConfigFromViper(nil)

	return func(ctx context.Context, state *agent.MetricsState) error {
		var jsonResult string
		var err error

		if pgConfig.IncludeQueries {
			// Query with query text
			query := `
/*dbtune*/
SELECT JSON_OBJECT_AGG(
	CONCAT(queryid, '_', userid, '_', dbid), 
	JSON_BUILD_OBJECT(
		'calls',calls,
		'total_exec_time',total_exec_time,
		'query_id', CONCAT(queryid, '_', userid, '_', dbid),
		'query',query,
		'rows',rows
	)
)
AS qrt_stats
FROM (SELECT * FROM pg_stat_statements
	WHERE query !~ 'BEGIN|COMMIT|\.pg_\.|\.dbtune\.|^SELECT \$1$|\.version\.'
	ORDER BY calls DESC)
AS f`
			err = pgPool.QueryRow(ctx, query).Scan(&jsonResult)
		} else {
			// Query without query text
			err = pgPool.QueryRow(ctx, PgStatStatementsQuery).Scan(&jsonResult)
		}

		if err != nil {
			return err
		}

		var queryStats map[string]utils.CachedPGStatStatement
		err = json.Unmarshal([]byte(jsonResult), &queryStats)
		if err != nil {
			return err
		}

		if len(queryStats) == 0 {
			return nil
		}

		if state.Cache.QueryRuntimeList == nil {
			state.Cache.QueryRuntimeList = queryStats
		} else {
			// Calculate the runtime of the queries (AQR)
			// Calculate the pg_stat_statements delta to send to the server
			pgStatStatementsDelta, totalDiffs := utils.CalculateQueryRuntimeDelta(state.Cache.QueryRuntimeList, queryStats)
			runtime := utils.CalculateQueryRuntime(state.Cache.QueryRuntimeList, queryStats)

			totalDiffsMetric, err := metrics.PGStatStatementsDeltaCount.AsFlatValue(totalDiffs)
			if err != nil {
				return err
			}

			pgStatStatementsDeltaMetric, err := metrics.PGStatStatementsDelta.AsFlatValue(pgStatStatementsDelta)
			if err != nil {
				return err
			}

			metricEntry, err := metrics.PerfAverageQueryRuntime.AsFlatValue(runtime)
			if err != nil {
				return err
			}

			// Only add metrics if they all succeeded to not have partial information

			if totalDiffs > 0 {
				state.AddMetric(pgStatStatementsDeltaMetric)
			}

			state.AddMetric(metricEntry)
			state.AddMetric(totalDiffsMetric)

			state.Cache.QueryRuntimeList = queryStats
		}

		return nil
	}
}

const ActiveConnectionsQuery = `
/*dbtune*/
SELECT COUNT(*) AS active_connections
FROM pg_stat_activity
WHERE state = 'active'
`

func ActiveConnections(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var result int
		err := pgPool.QueryRow(ctx, ActiveConnectionsQuery).Scan(&result)
		if err != nil {
			return err
		}

		metricEntry, err := metrics.PGActiveConnections.AsFlatValue(result)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		return nil
	}
}

const TransactionsPerSecondQuery = `
/*dbtune*/
SELECT SUM(xact_commit)::bigint
AS server_xact_commits
FROM pg_stat_database;
`

func TransactionsPerSecond(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var serverXactCommits int64
		err := pgPool.QueryRow(ctx, TransactionsPerSecondQuery).Scan(&serverXactCommits)
		if err != nil {
			return err
		}

		if state.Cache.XactCommit.Count == 0 {
			state.Cache.XactCommit = agent.XactStat{
				Count:     serverXactCommits,
				Timestamp: time.Now(),
			}
			return nil
		}

		if serverXactCommits == 0 {
			return nil
		}

		if serverXactCommits < state.Cache.XactCommit.Count {
			state.Cache.XactCommit = agent.XactStat{
				Count:     serverXactCommits,
				Timestamp: time.Now(),
			}
			return nil
		}

		// Calculate transactions per second
		duration := time.Since(state.Cache.XactCommit.Timestamp).Seconds()
		if duration > 0 {
			tps := float64(serverXactCommits-state.Cache.XactCommit.Count) / duration
			metricEntry, err := metrics.PerfTransactionsPerSecond.AsFlatValue(tps)
			if err != nil {
				return err
			}
			state.AddMetric(metricEntry)
		}

		// Update cache
		state.Cache.XactCommit = agent.XactStat{
			Count:     serverXactCommits,
			Timestamp: time.Now(),
		}

		return nil
	}
}

const DatabaseSizeQuery = `
/*dbtune*/
SELECT sum(pg_database_size(datname)) as total_size_bytes FROM pg_database;
`

// DatabaseSize returns the size of all the databases combined in bytes
func DatabaseSize(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var totalSizeBytes int64
		err := pgPool.QueryRow(ctx, DatabaseSizeQuery).Scan(&totalSizeBytes)
		if err != nil {
			return err
		}

		metricEntry, err := metrics.PGInstanceSize.AsFlatValue(totalSizeBytes)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		return nil
	}
}

// https://stackoverflow.com/a/25012622
const AutovacuumQuery = `
/*dbtune*/
SELECT COUNT(*) FROM pg_stat_activity WHERE query LIKE 'autovacuum:%';
`

func Autovacuum(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var result int
		err := pgPool.QueryRow(ctx, AutovacuumQuery).Scan(&result)
		if err != nil {
			return err
		}

		metricEntry, err := metrics.PGAutoVacuumCount.AsFlatValue(result)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		return nil
	}
}

const UptimeMinutesQuery = `
/*dbtune*/
SELECT EXTRACT(EPOCH FROM (current_timestamp - pg_postmaster_start_time())) / 60 as uptime_minutes;
`

func UptimeMinutes(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var uptime float64
		err := pgPool.QueryRow(ctx, UptimeMinutesQuery).Scan(&uptime)
		if err != nil {
			return err
		}

		metricEntry, err := metrics.ServerUptimeMinutes.AsFlatValue(uptime)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		return nil
	}
}

const WaitEventsQuery = `
/*dbtune*/
WITH RECURSIVE
current_waits AS (
	SELECT 
		wait_event_type,
		count(*) as count
	FROM pg_stat_activity
	WHERE wait_event_type IS NOT NULL
	GROUP BY wait_event_type
),
all_wait_types AS (
	VALUES 
		('Activity'),
		('BufferPin'),
		('Client'),
		('Extension'),
		('IO'),
		('IPC'),
		('Lock'),
		('LWLock'),
		('Timeout')
),
wait_counts AS (
	SELECT 
		awt.column1 as wait_event_type,
		COALESCE(cw.count, 0) as current_count
	FROM all_wait_types awt
	LEFT JOIN current_waits cw ON awt.column1 = cw.wait_event_type
)
SELECT 
	wait_event_type,
	current_count
FROM wait_counts
UNION ALL
SELECT 
	'TOTAL' as wait_event_type,
	sum(current_count) as current_count
FROM wait_counts;
`

func WaitEvents(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		rows, err := pgPool.Query(ctx, WaitEventsQuery)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var event string
			var count int
			err = rows.Scan(&event, &count)
			if err != nil {
				return err
			}

			metricEntry, _ := metrics.PGWaitEvent{Name: event}.AsFlatValue(count)
			state.AddMetric(metricEntry)
		}

		return nil
	}
}

const PGStatDatabaseQuery = `
/*dbtune*/
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

// PG stat database has info about IO, temp files and idle transaction time. Many of those are cumulative
// metrics and require calculating deltas between collection intervals.
// This function fetches and emits a number of those as well as calaculates the pg_cache_hit_ratio which
// is a derivative from blks_read and blks_hit
func PGStatDatabase(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var blksRead, blksHit, tupReturned, tupFetched, tupInserted, tupUpdated, tupDeleted, tempFiles, tempBytes, deadlocks int64
		var idleInTransactionTime float64
		err := pgPool.QueryRow(ctx, PGStatDatabaseQuery).Scan(
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

const PGStatUserTablesQuery = `
/*dbtune*/
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
		err := pgPool.QueryRow(ctx, PGStatUserTablesQuery).Scan(&jsonResult)
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

const PGStatBGWriterQuery = `
/*dbtune*/
SELECT
    buffers_clean,
	maxwritten_clean,
	buffers_alloc
FROM pg_stat_bgwriter;
`

// PGStatBGwriter collects statistics from pg_stat_bgwriter and computes and emits deltas for them.
func PGStatBGwriter(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var buffersClean, maxWrittenClean, buffersAlloc int64
		err := pgPool.QueryRow(ctx, PGStatBGWriterQuery).Scan(
			&buffersClean,
			&maxWrittenClean,
			&buffersAlloc,
		)

		if err != nil {
			return err
		}

		// Check if we have cached values from the previous collection
		mappings := []MetricMapping[int64]{
			{Current: &buffersClean, Previous: &state.Cache.PGBGWriter.BuffersClean, Metric: metrics.PGBGWBuffersClean},
			{Current: &maxWrittenClean, Previous: &state.Cache.PGBGWriter.MaxWrittenClean, Metric: metrics.PGMBGWaxWrittenClean},
			{Current: &buffersAlloc, Previous: &state.Cache.PGBGWriter.BuffersAlloc, Metric: metrics.PGBGWBuffersAlloc},
		}

		if err := EmitCumulativeMetricsMap(state, mappings); err != nil {
			return err
		}

		// Update cache for next iteration
		state.Cache.PGBGWriter = agent.PGBGWriter{
			BuffersClean:    buffersClean,
			MaxWrittenClean: maxWrittenClean,
			BuffersAlloc:    buffersAlloc,
			Timestamp:       time.Now(),
		}

		return nil
	}
}

const PGStatWalQuery = `
/*dbtune*/
SELECT
    wal_records,
	wal_fpi,
	wal_bytes,
	wal_buffers_full
FROM pg_stat_wal;
`

// PGStatWAL collects statistics from pg_stat_wal and computes and emits deltas for them.
func PGStatWAL(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var WalRecords, WalFpi, WalBytes, WalBuffersFull int64
		err := pgPool.QueryRow(ctx, PGStatWalQuery).Scan(
			&WalRecords,
			&WalFpi,
			&WalBytes,
			&WalBuffersFull,
		)

		if err != nil {
			return err
		}

		// Check if we have cached values from the previous collection
		mappingsInt := []MetricMapping[int64]{
			{Current: &WalRecords, Previous: &state.Cache.PGWAL.WALRecords, Metric: metrics.PGWALRecords},
			{Current: &WalFpi, Previous: &state.Cache.PGWAL.WALFpi, Metric: metrics.PGWALFpi},
			{Current: &WalBytes, Previous: &state.Cache.PGWAL.WALBytes, Metric: metrics.PGWALBytes},
			{Current: &WalBuffersFull, Previous: &state.Cache.PGWAL.WALBuffersFull, Metric: metrics.PGWALBuffersFull},
		}

		if err := EmitCumulativeMetricsMap(state, mappingsInt); err != nil {
			return err
		}

		// Update cache for next iteration
		state.Cache.PGWAL = agent.PGWAL{
			WALRecords:     WalRecords,
			WALFpi:         WalFpi,
			WALBytes:       WalBytes,
			WALBuffersFull: WalBuffersFull,
			Timestamp:      time.Now(),
		}

		return nil
	}
}

const PGStatCheckpointerQuery = `
/*dbtune*/
SELECT
    num_timed,
	num_requested,
	write_time,
	sync_time,
	buffers_written
FROM pg_stat_checkpointer
`

// PG stat database has info about IO, temp files and idle temp time. Many of those are cumulative
// metrics and require
func PGStatCheckpointer(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var numTimed, numRequested, BuffersWritten int64
		var writeTime, syncTime float64
		err := pgPool.QueryRow(ctx, PGStatCheckpointerQuery).Scan(
			&numTimed,
			&numRequested,
			&writeTime,
			&syncTime,
			&BuffersWritten,
		)

		if err != nil {
			return err
		}

		// Check if we have cached values from the previous collection
		mappingsInt := []MetricMapping[int64]{
			{Current: &numTimed, Previous: &state.Cache.PGCheckPointer.NumTimed, Metric: metrics.PGCPNumTimed},
			{Current: &numRequested, Previous: &state.Cache.PGCheckPointer.NumRequested, Metric: metrics.PGCPNumRequested},
			{Current: &BuffersWritten, Previous: &state.Cache.PGCheckPointer.BuffersWritten, Metric: metrics.PGCPBuffersWritten},
		}
		mappingsFloat := []MetricMapping[float64]{
			{Current: &writeTime, Previous: &state.Cache.PGCheckPointer.WriteTime, Metric: metrics.PGCPWriteTime},
			{Current: &syncTime, Previous: &state.Cache.PGCheckPointer.SyncTime, Metric: metrics.PGCPSyncTime},
		}

		if err := EmitCumulativeMetricsMap(state, mappingsInt); err != nil {
			return err
		}
		if err := EmitCumulativeMetricsMap(state, mappingsFloat); err != nil {
			return err
		}

		// Update cache for next iteration
		state.Cache.PGCheckPointer = agent.PGCheckPointer{
			NumTimed:       numTimed,
			NumRequested:   numRequested,
			WriteTime:      writeTime,
			SyncTime:       syncTime,
			BuffersWritten: BuffersWritten,
			Timestamp:      time.Now(),
		}

		return nil
	}
}
