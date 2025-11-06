package utils

import (
	"context"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DBTuneQueryPrefix is the comment prefix added to all dbtune queries
	// to identify and filter them in pg_stat_statements
	DBTuneQueryPrefix = "/*dbtune*/"
)

var diffLimit = 500

// addPrefixToQuery trims leading whitespace from the query and ensures
// it starts with the dbtune prefix to prevent filtering issues.
func addPrefixToQuery(query string) string {
	trimmedQuery := strings.TrimSpace(query)
	if !strings.HasPrefix(trimmedQuery, DBTuneQueryPrefix) {
		trimmedQuery = DBTuneQueryPrefix + " " + trimmedQuery
	}
	return trimmedQuery
}

// QueryWithPrefix executes a query with the dbtune prefix.
func QueryWithPrefix(pool *pgxpool.Pool, ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	return pool.Query(ctx, addPrefixToQuery(query), args...)
}

// QueryRowWithPrefix executes a query that returns a single row with the dbtune prefix.
func QueryRowWithPrefix(pool *pgxpool.Pool, ctx context.Context, query string, args ...any) pgx.Row {
	return pool.QueryRow(ctx, addPrefixToQuery(query), args...)
}

// ExecWithPrefix executes a command with the dbtune prefix.
func ExecWithPrefix(pool *pgxpool.Pool, ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	return pool.Exec(ctx, addPrefixToQuery(query), args...)
}

type CachedPGStatStatement struct {
	QueryID       string  `json:"query_id"`
	Query         string  `json:"query,omitempty"`
	Calls         int     `json:"calls"`
	TotalExecTime float64 `json:"total_exec_time"`
	Rows          int64   `json:"rows"`
}

// CalculateQueryRuntime calculates the runtime of
// a query based on two consecutive snapshots of the pg_stat_statements table
// and returns the runtime in milliseconds.
func CalculateQueryRuntime(prev, curr map[string]CachedPGStatStatement) float64 {
	totalExecTime := 0.0
	totalCalls := 0

	// Iterate over the current snapshot
	for queryId, currStat := range curr {
		// Get the previous stats, defaulting to zero if not found
		prevStat, exists := prev[queryId]
		if !exists {
			prevStat = CachedPGStatStatement{
				QueryID:       queryId,
				Query:         currStat.Query,
				Calls:         0,
				TotalExecTime: 0.0,
				Rows:          0,
			}
		}

		// Calculate the difference in calls and execution time
		queryIdCalls := currStat.Calls - prevStat.Calls
		queryIdExecTime := currStat.TotalExecTime - prevStat.TotalExecTime

		// Only consider queries that have had calls and execution time increase
		// There might be edge cases where the query has been reset, but we ignore those
		if queryIdCalls > 0 && queryIdExecTime > 0 {
			totalCalls += queryIdCalls
			totalExecTime += queryIdExecTime
		}
	}

	// Return the average execution time in milliseconds
	if totalCalls == 0 {
		return 0.0
	}
	return totalExecTime / float64(totalCalls)
}

// CalculateQueryRuntimeDelta calculates the delta
// between two consecutive snapshots of the pg_stat_statements and returns:
// [{ query_id: "query_id", calls: 10, total_exec_time: 1000 }, ...]
// The diff will be limited to only 100 different queries changed by default.
// Also, it will return the total number of diffs found to give us an idea if a lot of information is not captured
// The cap will be based upon their average execution time (descending)
func CalculateQueryRuntimeDelta(prev, curr map[string]CachedPGStatStatement) ([]CachedPGStatStatement, int) {
	diffs := []CachedPGStatStatement{}
	totalDiffs := 0

	// Calculate diffs for all queries in current snapshot
	for queryId, currStat := range curr {
		// Get the previous stats, defaulting to zero if not found
		prevStat, exists := prev[queryId]
		if !exists {
			prevStat = CachedPGStatStatement{QueryID: queryId, Calls: 0, TotalExecTime: 0.0, Rows: 0}
		}

		// Calculate the difference in calls and execution time
		callsDiff := currStat.Calls - prevStat.Calls
		execTimeDiff := currStat.TotalExecTime - prevStat.TotalExecTime
		rowsDiff := currStat.Rows - prevStat.Rows

		// Only consider queries that have had positive changes
		if callsDiff > 0 && execTimeDiff > 0 && rowsDiff > 0 {
			totalDiffs++

			diffs = append(diffs, CachedPGStatStatement{
				QueryID:       queryId,
				Query:         currStat.Query,
				Calls:         callsDiff,
				TotalExecTime: execTimeDiff,
				Rows:          rowsDiff,
			})
		}
	}

	// Sort diffs by average execution time (highest first)
	sort.Slice(diffs, func(i, j int) bool {
		avgI := diffs[i].TotalExecTime / float64(diffs[i].Calls)
		avgJ := diffs[j].TotalExecTime / float64(diffs[j].Calls)
		return avgI > avgJ
	})

	// Limit entries based on the specified limit
	if len(diffs) > diffLimit {
		diffs = diffs[:diffLimit]
	}

	return diffs, totalDiffs
}
