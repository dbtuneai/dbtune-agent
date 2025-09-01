package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const PgStatStatementsQuery = `
/*dbtune*/
SELECT JSON_OBJECT_AGG(
	CONCAT(queryid, '_', userid, '_', dbid), 
	JSON_BUILD_OBJECT(
		'calls',calls,
		'total_exec_time',total_exec_time,
		'query_id', CONCAT(queryid, '_', userid, '_', dbid)
	)
)
AS qrt_stats
FROM (SELECT * FROM pg_stat_statements WHERE query NOT LIKE '%dbtune%' ORDER BY calls DESC)
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
		'query',query
	)
)
AS qrt_stats
FROM (SELECT * FROM pg_stat_statements WHERE query NOT LIKE '%dbtune%' ORDER BY calls DESC)
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

		if state.Cache.QueryRuntimeList == nil {
			state.Cache.QueryRuntimeList = queryStats
		} else {
			// Calculate the runtime of the queries (AQR)
			runtime := utils.CalculateQueryRuntime(state.Cache.QueryRuntimeList, queryStats)

			metricEntry, err := metrics.PerfAverageQueryRuntime.AsFlatValue(runtime)
			if err != nil {
				return err
			}
			state.AddMetric(metricEntry)

			// Calculate the pg_stat_statements delta to send to the server
			pgStatStatementsDelta, totalDiffs := utils.CalculateQueryRuntimeDelta(state.Cache.QueryRuntimeList, queryStats)

			totalDiffsMetric, _ := metrics.PGStatStatementsDeltaCount.AsFlatValue(totalDiffs)
			state.AddMetric(totalDiffsMetric)

			pgStatStatementsDeltaMetric, _ := metrics.PGStatStatementsDelta.AsFlatValue(pgStatStatementsDelta)
			state.AddMetric(pgStatStatementsDeltaMetric)

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

		metricEntry, err := metrics.PGAutovacuumCount.AsFlatValue(result)
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

const BufferCacheHitRatioQuery = `
/*dbtune*/
SELECT blks_hit, blks_read
FROM pg_stat_database 
WHERE datname = current_database();
`

// BufferCacheHitRatio returns the buffer cache hit ratio based on delta calculations.
// This implementation calculates the hit ratio based on the difference between
// two consecutive snapshots, providing a more meaningful metric for recent performance.
func BufferCacheHitRatio(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var blksHit, blksRead int64
		err := pgPool.QueryRow(ctx, BufferCacheHitRatioQuery).Scan(&blksHit, &blksRead)
		if err != nil {
			return err
		}

		// Validate that we have reasonable values
		if blksHit < 0 || blksRead < 0 {
			return fmt.Errorf("invalid buffer cache statistics: blks_hit=%d, blks_read=%d", blksHit, blksRead)
		}

		// Check if we have cached values from the previous collection
		if state.Cache.BufferStats.BlksHit == 0 && state.Cache.BufferStats.BlksRead == 0 {
			// First collection, just cache the values
			state.Cache.BufferStats = agent.BufferStat{
				BlksHit:   blksHit,
				BlksRead:  blksRead,
				Timestamp: time.Now(),
			}
			return nil
		}

		// Calculate deltas
		blksHitDelta := blksHit - state.Cache.BufferStats.BlksHit
		blksReadDelta := blksRead - state.Cache.BufferStats.BlksRead

		// Handle counter resets (e.g., after PostgreSQL crash or immediate shutdown)
		if blksHitDelta < 0 || blksReadDelta < 0 {
			// Reset detected, update cache and skip this collection
			state.Cache.BufferStats = agent.BufferStat{
				BlksHit:   blksHit,
				BlksRead:  blksRead,
				Timestamp: time.Now(),
			}
			return nil
		}

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

		metricEntry, err := metrics.PGCacheHitRatio.AsFlatValue(bufferCacheHitRatio)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		// Update cache for next iteration
		state.Cache.BufferStats = agent.BufferStat{
			BlksHit:   blksHit,
			BlksRead:  blksRead,
			Timestamp: time.Now(),
		}

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
