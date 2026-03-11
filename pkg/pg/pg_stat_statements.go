package pg

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatStatementsQueryBase queries pg_stat_statements excluding system queries.
// Filters out: dbtune queries (using fast starts_with), transactions (BEGIN/COMMIT/ROLLBACK),
// pg_* system queries, parameterized health checks (SELECT $1), version checks, SET/SHOW statements
var PgStatStatementsQueryBase = fmt.Sprintf(`
SELECT
	queryid,
	userid,
	dbid,
	calls,
	total_exec_time,
	rows
FROM pg_stat_statements
WHERE NOT starts_with(query, '%s')
  AND query !~* '^\s*(BEGIN|COMMIT|ROLLBACK|SET |SHOW |SELECT (pg_|\$1$|version\s*\(\s*\)))\s*;?\s*$'
`, utils.DBtuneQueryPrefix)

// PgStatStatementsQueryWithTextFmt is like PgStatStatementsQueryBase but includes query text.
// The first %s is replaced with the dbtune prefix (/*dbtune*/)
// The %%d is a format placeholder for the maximum query text length (filled in at runtime)
var PgStatStatementsQueryWithTextFmt = fmt.Sprintf(`
SELECT
	queryid,
	userid,
	dbid,
	calls,
	total_exec_time,
	rows,
	LEFT(query, %%d) as query,
	LENGTH(query)
FROM pg_stat_statements
WHERE NOT starts_with(query, '%s')
  AND query !~* '^\s*(BEGIN|COMMIT|ROLLBACK|SET |SHOW |SELECT (pg_|\$1$|version\s*\(\s*\)))\s*;?\s*$'
`, utils.DBtuneQueryPrefix)

func PGStatStatements(pgPool *pgxpool.Pool, includeQueries bool, maxQueryTextLength int) func(ctx context.Context, state *agent.MetricsState) error {
	// Build query based on config
	query := PgStatStatementsQueryBase
	if includeQueries {
		query = fmt.Sprintf(PgStatStatementsQueryWithTextFmt, maxQueryTextLength)
	}

	return func(ctx context.Context, state *agent.MetricsState) error {
		rows, err := utils.QueryWithPrefix(pgPool, ctx, query)

		if err != nil {
			return err
		}
		defer rows.Close()

		// Build map from rows
		queryStats := make(map[string]utils.CachedPGStatStatement)
		for rows.Next() {
			// queryid is bigint (int64) - can be negative hash value
			// userid and dbid are OID types - must use uint32 for proper scanning
			//
			// NOTE(eddie): Because pg_stat_statments is a view, it's possible that all columns could be
			// null (they shouldn't be, but as soon as one of them is, this will constantly break)
			// THIS IS NOT THE EXPECTED CASE, but can happen.
			var queryid *int64
			var userid, dbid *uint32
			var calls *int
			var totalExecTime *float64
			var rowCount *int64
			var query *string
			var querylen *int

			if includeQueries {
				err = rows.Scan(&queryid, &userid, &dbid, &calls, &totalExecTime, &rowCount, &query, &querylen)
			} else {
				err = rows.Scan(&queryid, &userid, &dbid, &calls, &totalExecTime, &rowCount)
			}

			if err != nil {
				return err
			}

			// These we use to build an identifier
			identifiersHasNull := (queryid == nil || userid == nil || dbid == nil)

			// These we use for calculations
			fieldValuesHasNull := (calls == nil || totalExecTime == nil || rowCount == nil)

			if identifiersHasNull || fieldValuesHasNull {
				continue
			}

			// Construct composite key
			compositeKey := fmt.Sprintf("%d_%d_%d", *queryid, *userid, *dbid)

			// Build the struct
			stat := utils.CachedPGStatStatement{
				QueryID:       compositeKey,
				Calls:         *calls,
				TotalExecTime: *totalExecTime,
				Rows:          *rowCount,
			}

			if includeQueries && query != nil {
				stat.Query = *query
				stat.QueryLen = *querylen
			}

			queryStats[compositeKey] = stat
		}

		// Check for errors from iterating over rows
		if err = rows.Err(); err != nil {
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
