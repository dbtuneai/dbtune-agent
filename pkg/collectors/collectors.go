package collectors

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
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
	return func(ctx context.Context, state *agent.MetricsState) error {
		var jsonResult string
		err := pgPool.QueryRow(ctx, PgStatStatementsQuery).Scan(&jsonResult)
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

			metricEntry, err := utils.NewMetric("perf_average_query_runtime", runtime, utils.Float)
			if err != nil {
				return err
			}
			state.AddMetric(metricEntry)

			// Calculate the pg_stat_statements delta to send to the server
			pgStatStatementsDelta, totalDiffs := utils.CalculateQueryRuntimeDelta(state.Cache.QueryRuntimeList, queryStats)

			totalDiffsMetric, _ := utils.NewMetric("pg_stat_statements_delta_count", totalDiffs, utils.Int)
			state.AddMetric(totalDiffsMetric)

			pgStatStatementsDeltaMetric, _ := utils.NewMetric("pg_stat_statements_delta", pgStatStatementsDelta, utils.PgssDelta)
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

		metricEntry, err := utils.NewMetric("pg_active_connections", result, utils.Int)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		return nil
	}
}

const SleepQuery = `
/*dbtune*/
SELECT pg_sleep(1000000);
`

func ArtificiallyFailingQueries(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	// Perform runtime reflection to make sure that the
	// struct is embedding the default adapter
	return func(ctx context.Context, state *agent.MetricsState) error {
		var result int
		err := pgPool.QueryRow(ctx, SleepQuery).Scan(&result)
		if err != nil {
			return err
		}

		metricEntry, err := utils.NewMetric("pg_sleep_result", result, utils.Int)
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
			metricEntry, err := utils.NewMetric("perf_transactions_per_second", tps, utils.Float)
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

		metricEntry, err := utils.NewMetric("pg_instance_size", totalSizeBytes, utils.Int)
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

		metricEntry, err := utils.NewMetric("pg_autovacuum_count", result, utils.Int)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		return nil
	}
}

const UptimeQuery = `
/*dbtune*/
SELECT EXTRACT(EPOCH FROM (current_timestamp - pg_postmaster_start_time())) / 60 as uptime_minutes;
`

func Uptime(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var uptime float64
		err := pgPool.QueryRow(ctx, UptimeQuery).Scan(&uptime)
		if err != nil {
			return err
		}

		metricEntry, err := utils.NewMetric("server_uptime", uptime, utils.Float)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		return nil
	}
}

const BufferCacheHitRatioQuery = `
/*dbtune*/
SELECT ROUND(100.0 * blks_hit / (blks_hit + blks_read), 2) as cache_hit_ratio
FROM pg_stat_database 
WHERE datname = current_database();
`

// BufferCacheHitRatio returns the buffer cache hit ratio.
// The current implementation gets only the hit ratio for the current database,
// we intentionally avoid aggregating the hit ratio for all databases,
// as this may add much noise from unused DBs.
func BufferCacheHitRatio(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var bufferCacheHitRatio float64
		err := pgPool.QueryRow(ctx, BufferCacheHitRatioQuery).Scan(&bufferCacheHitRatio)
		if err != nil {
			return err
		}

		metricEntry, err := utils.NewMetric("pg_cache_hit_ratio", bufferCacheHitRatio, utils.Float)
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

			metricEntry, _ := utils.NewMetric(fmt.Sprintf("pg_wait_events_%s", strings.ToLower(event)), count, utils.Int)
			state.AddMetric(metricEntry)
		}

		return nil
	}
}

func HardwareInfoOnPremise() func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		cpuPercentage, _ := cpu.Percent(time.Millisecond*100, false) // Report the average CPU usage over 100ms
		cpuModelMetric, _ := utils.NewMetric("node_cpu_usage", cpuPercentage[0], utils.Float)
		state.AddMetric(cpuModelMetric)

		// Get Reads and Write IOps
		ioCounters, _ := disk.IOCounters()
		// Get the total Read and Write IOps
		// Update the cache state, and on the next iteration,
		// calculate the difference
		var writes, reads uint64

		for _, ioCounter := range ioCounters {
			writes += ioCounter.WriteCount
			reads += ioCounter.ReadCount
		}

		if state.Cache.IOCountersStat != (agent.IOCounterStat{}) {
			totalIOps := (reads + writes) - (state.Cache.IOCountersStat.ReadCount + state.Cache.IOCountersStat.WriteCount)
			iopsTotalMetric, _ := utils.NewMetric("node_disk_io_ops_total", totalIOps, utils.Int)
			state.AddMetric(iopsTotalMetric)

			readCountMetric, _ := utils.NewMetric("node_disk_io_ops_read", reads-state.Cache.IOCountersStat.ReadCount, utils.Int)
			state.AddMetric(readCountMetric)

			writeCountMetric, _ := utils.NewMetric("node_disk_io_ops_write", writes-state.Cache.IOCountersStat.WriteCount, utils.Int)
			state.AddMetric(writeCountMetric)
		}

		// Update cache
		state.Cache.IOCountersStat = agent.IOCounterStat{ReadCount: reads, WriteCount: writes}

		// Memory usage
		memoryInfo, _ := mem.VirtualMemory()
		usedMemory, _ := utils.NewMetric("node_memory_used", memoryInfo.Total-memoryInfo.Available, utils.Int)
		state.AddMetric(usedMemory)

		return nil
	}
}

// PGVersion returns the version of the PostgreSQL instance
const PGVersionQuery = `
/*dbtune*/
SELECT version();
`

// Example: 16.4
func PGVersion(pgPool *pgxpool.Pool) (string, error) {
	var pgVersion string
	versionRegex := regexp.MustCompile(`PostgreSQL (\d+\.\d+)`)
	err := pgPool.QueryRow(context.Background(), PGVersionQuery).Scan(&pgVersion)
	if err != nil {
		return "", err
	}
	matches := versionRegex.FindStringSubmatch(pgVersion)

	return matches[1], nil
}

const MaxConnectionsQuery = `
/*dbtune*/
SELECT setting::integer FROM pg_settings WHERE  name = 'max_connections';
`

func MaxConnections(pgPool *pgxpool.Pool) (uint32, error) {
	var maxConnections uint32
	err := pgPool.QueryRow(context.Background(), MaxConnectionsQuery).Scan(&maxConnections)
	if err != nil {
		return 0, fmt.Errorf("error getting max connections: %v", err)
	}

	return maxConnections, nil
}
