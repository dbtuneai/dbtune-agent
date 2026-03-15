package queries

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatStatementsName     = "pg_stat_statements"
	PgStatStatementsInterval = 1 * time.Minute
)

// pgStatStatementsFilter is the WHERE clause shared by all pg_stat_statements queries.
// It filters out dbtune queries and system noise.
var pgStatStatementsFilter = fmt.Sprintf(`
WHERE NOT starts_with(query, '%s')
  AND query !~* '^\s*(BEGIN|COMMIT|ROLLBACK|SET |SHOW |SELECT (pg_|\$1$|version\s*\(\s*\)))\s*;?\s*$'
`, utils.DBtuneQueryPrefix)

// pgStatStatementsDiffLimit is the max number of delta entries to include.
var pgStatStatementsDiffLimit = 500

// PgStatStatementsRow represents a single row from pg_stat_statements.
// All fields are nullable pointers; the scanner silently skips columns
// not present in the current PG version.
type PgStatStatementsRow struct {
	// Identifiers
	UserID  *Oid    `json:"userid" db:"userid"`
	DbID    *Oid    `json:"dbid" db:"dbid"`
	QueryID *Bigint `json:"queryid" db:"queryid"`

	// Query text
	Query    *Text   `json:"query,omitempty" db:"query"`
	QueryLen *Bigint `json:"query_len,omitempty" db:"query_len"` // computed: LENGTH(query)

	// Core counters (all versions)
	Calls          *Bigint          `json:"calls" db:"calls"`
	TotalExecTime  *DoublePrecision `json:"total_exec_time" db:"total_exec_time"`
	MinExecTime    *DoublePrecision `json:"min_exec_time" db:"min_exec_time"`
	MaxExecTime    *DoublePrecision `json:"max_exec_time" db:"max_exec_time"`
	MeanExecTime   *DoublePrecision `json:"mean_exec_time" db:"mean_exec_time"`
	StddevExecTime *DoublePrecision `json:"stddev_exec_time" db:"stddev_exec_time"`
	Rows           *Bigint          `json:"rows" db:"rows"`

	// Block I/O (all versions)
	SharedBlksHit     *Bigint `json:"shared_blks_hit" db:"shared_blks_hit"`
	SharedBlksRead    *Bigint `json:"shared_blks_read" db:"shared_blks_read"`
	SharedBlksDirtied *Bigint `json:"shared_blks_dirtied" db:"shared_blks_dirtied"`
	SharedBlksWritten *Bigint `json:"shared_blks_written" db:"shared_blks_written"`
	LocalBlksHit      *Bigint `json:"local_blks_hit" db:"local_blks_hit"`
	LocalBlksRead     *Bigint `json:"local_blks_read" db:"local_blks_read"`
	LocalBlksDirtied  *Bigint `json:"local_blks_dirtied" db:"local_blks_dirtied"`
	LocalBlksWritten  *Bigint `json:"local_blks_written" db:"local_blks_written"`
	TempBlksRead      *Bigint `json:"temp_blks_read" db:"temp_blks_read"`
	TempBlksWritten   *Bigint `json:"temp_blks_written" db:"temp_blks_written"`

	// Block I/O timing — called blk_read_time/blk_write_time in PG13-16,
	// renamed to shared_blk_read_time/shared_blk_write_time in PG17.
	// We always use the PG17 name; the query aliases the old name for PG13-16.
	SharedBlkReadTime  *DoublePrecision `json:"shared_blk_read_time" db:"shared_blk_read_time"`
	SharedBlkWriteTime *DoublePrecision `json:"shared_blk_write_time" db:"shared_blk_write_time"`

	// PG17+: local block I/O timing (new in PG17, no equivalent in older versions)
	LocalBlkReadTime  *DoublePrecision `json:"local_blk_read_time,omitempty" db:"local_blk_read_time"`
	LocalBlkWriteTime *DoublePrecision `json:"local_blk_write_time,omitempty" db:"local_blk_write_time"`

	// PG13+
	Plans          *Bigint          `json:"plans" db:"plans"`
	TotalPlanTime  *DoublePrecision `json:"total_plan_time" db:"total_plan_time"`
	MinPlanTime    *DoublePrecision `json:"min_plan_time" db:"min_plan_time"`
	MaxPlanTime    *DoublePrecision `json:"max_plan_time" db:"max_plan_time"`
	MeanPlanTime   *DoublePrecision `json:"mean_plan_time" db:"mean_plan_time"`
	StddevPlanTime *DoublePrecision `json:"stddev_plan_time" db:"stddev_plan_time"`
	WalRecords     *Bigint          `json:"wal_records" db:"wal_records"`
	WalFpi         *Bigint          `json:"wal_fpi" db:"wal_fpi"`
	WalBytes       *Numeric         `json:"wal_bytes" db:"wal_bytes"`

	// PG14+
	TopLevel *Boolean `json:"toplevel" db:"toplevel"`

	// PG15+
	TempBlkReadTime  *DoublePrecision `json:"temp_blk_read_time" db:"temp_blk_read_time"`
	TempBlkWriteTime *DoublePrecision `json:"temp_blk_write_time" db:"temp_blk_write_time"`

	JitFunctions         *Bigint          `json:"jit_functions" db:"jit_functions"`
	JitGenerationTime    *DoublePrecision `json:"jit_generation_time" db:"jit_generation_time"`
	JitInliningCount     *Bigint          `json:"jit_inlining_count" db:"jit_inlining_count"`
	JitInliningTime      *DoublePrecision `json:"jit_inlining_time" db:"jit_inlining_time"`
	JitOptimizationCount *Bigint          `json:"jit_optimization_count" db:"jit_optimization_count"`
	JitOptimizationTime  *DoublePrecision `json:"jit_optimization_time" db:"jit_optimization_time"`
	JitEmissionCount     *Bigint          `json:"jit_emission_count" db:"jit_emission_count"`
	JitEmissionTime      *DoublePrecision `json:"jit_emission_time" db:"jit_emission_time"`
}

// PgStatStatementsDelta holds the per-query diff between two consecutive snapshots.
// Query text is not included — it can be looked up via the identifiers in Rows.
type PgStatStatementsDelta struct {
	// Identifiers
	UserID  *Oid    `json:"userid" db:"userid"`
	DbID    *Oid    `json:"dbid" db:"dbid"`
	QueryID *Bigint `json:"queryid" db:"queryid"`

	// Diffed counters
	Calls         *Bigint          `json:"calls" db:"calls"`
	TotalExecTime *DoublePrecision `json:"total_exec_time" db:"total_exec_time"`
	Rows          *Bigint          `json:"rows" db:"rows"`
}

// PgStatStatementsPayload is the JSON body POSTed to /api/v1/agent/pg_stat_statements.
type PgStatStatementsPayload struct {
	Rows                []PgStatStatementsRow   `json:"rows"`
	Deltas              []PgStatStatementsDelta `json:"deltas,omitempty"`
	DeltaCount          int                     `json:"delta_count"`
	AverageQueryRuntime float64                 `json:"average_query_runtime"`
}

// buildPgStatStatementsQuery returns the SQL query for pg_stat_statements,
// building a version-specific column list to handle columns that were added
// or renamed across PG versions, and handling query text inclusion/truncation
// at the SQL level to avoid transferring huge strings over the wire.
//
// Version differences:
//   - PG14+: adds toplevel
//   - PG15+: adds temp_blk_read_time, temp_blk_write_time, jit_* fields
//   - PG17+: renames blk_read_time → shared_blk_read_time, blk_write_time → shared_blk_write_time;
//     adds local_blk_read_time, local_blk_write_time (genuinely new)
func buildPgStatStatementsQuery(includeQueries bool, maxQueryTextLength int, pgVersion int) string {
	var cols []string

	// Identifiers
	cols = append(cols, "userid", "dbid", "queryid")

	// Query text handling
	if !includeQueries {
		cols = append(cols, "NULL::text AS query", "NULL::bigint AS query_len")
	} else {
		cols = append(cols,
			fmt.Sprintf("LEFT(query, %d) AS query", maxQueryTextLength),
			"LENGTH(query) AS query_len",
		)
	}

	// Core counters (all versions)
	cols = append(cols,
		"calls", "total_exec_time", "min_exec_time", "max_exec_time",
		"mean_exec_time", "stddev_exec_time", "rows",
	)

	// Block I/O counters (all versions)
	cols = append(cols,
		"shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written",
		"local_blks_hit", "local_blks_read", "local_blks_dirtied", "local_blks_written",
		"temp_blks_read", "temp_blks_written",
	)

	// blk_read_time/blk_write_time were renamed to shared_blk_read_time/shared_blk_write_time
	// in PG17 (pgss 1.11). We always use the PG17 name; alias the old name for PG13-16.
	if pgVersion >= 17 {
		cols = append(cols, "shared_blk_read_time", "shared_blk_write_time")
		// PG17 also added local block I/O timing (genuinely new, no PG16 equivalent)
		cols = append(cols, "local_blk_read_time", "local_blk_write_time")
	} else {
		cols = append(cols,
			"blk_read_time AS shared_blk_read_time",
			"blk_write_time AS shared_blk_write_time",
		)
	}

	// PG13+: plan stats, WAL
	cols = append(cols,
		"plans", "total_plan_time", "min_plan_time", "max_plan_time",
		"mean_plan_time", "stddev_plan_time",
		"wal_records", "wal_fpi", "wal_bytes",
	)

	// PG14+: toplevel
	if pgVersion >= 14 {
		cols = append(cols, "toplevel")
	}

	// PG15+: temp block timing, JIT stats
	if pgVersion >= 15 {
		cols = append(cols,
			"temp_blk_read_time", "temp_blk_write_time",
			"jit_functions", "jit_generation_time",
			"jit_inlining_count", "jit_inlining_time",
			"jit_optimization_count", "jit_optimization_time",
			"jit_emission_count", "jit_emission_time",
		)
	}

	return fmt.Sprintf("SELECT %s\nFROM pg_stat_statements\n%s",
		strings.Join(cols, ", "), pgStatStatementsFilter)
}

// compositeKey returns the snapshot map key for a row.
func compositeKey(r *PgStatStatementsRow) string {
	var qid, uid, did int64
	if r.QueryID != nil {
		qid = int64(*r.QueryID)
	}
	if r.UserID != nil {
		uid = int64(*r.UserID)
	}
	if r.DbID != nil {
		did = int64(*r.DbID)
	}
	return fmt.Sprintf("%d_%d_%d", qid, uid, did)
}

// toSnapshot converts a slice of rows into a map keyed by composite key.
func toSnapshot(rows []PgStatStatementsRow) map[string]PgStatStatementsRow {
	m := make(map[string]PgStatStatementsRow, len(rows))
	for _, r := range rows {
		if r.QueryID == nil || r.UserID == nil || r.DbID == nil {
			continue
		}
		m[compositeKey(&r)] = r
	}
	return m
}

// bigintDiff returns curr - prev if both are non-nil and the result is positive.
// Returns nil otherwise (indicates reset or missing data).
func bigintDiff(prev, curr *Bigint) *Bigint {
	if prev == nil || curr == nil {
		return nil
	}
	d := Bigint(int64(*curr) - int64(*prev))
	if d < 0 {
		return nil
	}
	return &d
}

// doubleDiff returns curr - prev if both are non-nil and the result is positive.
func doubleDiff(prev, curr *DoublePrecision) *DoublePrecision {
	if prev == nil || curr == nil {
		return nil
	}
	d := DoublePrecision(float64(*curr) - float64(*prev))
	if d < 0 {
		return nil
	}
	return &d
}

// calculateStatementDeltas computes per-query diffs between two snapshots.
// Only diffs the three original counters: calls, total_exec_time, rows.
// All other fields are copied from the current snapshot as-is.
// Only includes queries where all three counters increased.
// Sorts by avg exec time desc and caps at pgStatStatementsDiffLimit.
// Returns the delta rows and the total number of qualifying diffs (before cap).
func calculateStatementDeltas(prev, curr map[string]PgStatStatementsRow) ([]PgStatStatementsDelta, int) {
	var diffs []PgStatStatementsDelta
	totalDiffs := 0

	for key, currRow := range curr {
		prevRow, exists := prev[key]

		callsDiff := bigintDiff(zeroIfNil(prevRow.Calls, exists), currRow.Calls)
		execTimeDiff := doubleDiff(zeroIfNilDouble(prevRow.TotalExecTime, exists), currRow.TotalExecTime)
		rowsDiff := bigintDiff(zeroIfNil(prevRow.Rows, exists), currRow.Rows)

		// Only include if all three counters increased
		if callsDiff == nil || *callsDiff <= 0 ||
			execTimeDiff == nil || *execTimeDiff <= 0 ||
			rowsDiff == nil || *rowsDiff <= 0 {
			continue
		}

		totalDiffs++

		diffs = append(diffs, PgStatStatementsDelta{
			UserID:        currRow.UserID,
			DbID:          currRow.DbID,
			QueryID:       currRow.QueryID,
			Calls:         callsDiff,
			TotalExecTime: execTimeDiff,
			Rows:          rowsDiff,
		})
	}

	// Sort by avg exec time desc
	sort.Slice(diffs, func(i, j int) bool {
		avgI := float64(*diffs[i].TotalExecTime) / float64(*diffs[i].Calls)
		avgJ := float64(*diffs[j].TotalExecTime) / float64(*diffs[j].Calls)
		return avgI > avgJ
	})

	if len(diffs) > pgStatStatementsDiffLimit {
		diffs = diffs[:pgStatStatementsDiffLimit]
	}

	return diffs, totalDiffs
}

// calculateAvgRuntime computes the average query runtime across all queries
// that had increased calls and execution time between two snapshots.
func calculateAvgRuntime(prev, curr map[string]PgStatStatementsRow) float64 {
	totalExecTime := 0.0
	totalCalls := int64(0)

	for key, currRow := range curr {
		if currRow.Calls == nil || currRow.TotalExecTime == nil {
			continue
		}

		prevRow, exists := prev[key]
		var prevCalls int64
		var prevExecTime float64
		if exists && prevRow.Calls != nil {
			prevCalls = int64(*prevRow.Calls)
		}
		if exists && prevRow.TotalExecTime != nil {
			prevExecTime = float64(*prevRow.TotalExecTime)
		}

		callsDiff := int64(*currRow.Calls) - prevCalls
		execTimeDiff := float64(*currRow.TotalExecTime) - prevExecTime

		if callsDiff > 0 && execTimeDiff > 0 {
			totalCalls += callsDiff
			totalExecTime += execTimeDiff
		}
	}

	if totalCalls == 0 {
		return 0.0
	}
	return totalExecTime / float64(totalCalls)
}

// zeroIfNil returns a zero Bigint pointer if the row didn't exist in prev,
// or the actual value if it did.
func zeroIfNil(val *Bigint, exists bool) *Bigint {
	if !exists {
		zero := Bigint(0)
		return &zero
	}
	return val
}

func zeroIfNilDouble(val *DoublePrecision, exists bool) *DoublePrecision {
	if !exists {
		zero := DoublePrecision(0)
		return &zero
	}
	return val
}

// pgMajorVersionRegex extracts the major version from SELECT version() output.
var pgMajorVersionRegex = regexp.MustCompile(`PostgreSQL (\d+)`)

// queryPGMajorVersion returns the PG major version (e.g. 17) by querying the server.
func queryPGMajorVersion(pool *pgxpool.Pool, ctx context.Context) (int, error) {
	var versionStr string
	err := utils.QueryRowWithPrefix(pool, ctx, PGVersionQuery).Scan(&versionStr)
	if err != nil {
		return 0, fmt.Errorf("failed to query PG version: %w", err)
	}
	matches := pgMajorVersionRegex.FindStringSubmatch(versionStr)
	if len(matches) < 2 {
		return 0, fmt.Errorf("could not parse PG version from %q", versionStr)
	}
	var major int
	fmt.Sscanf(matches[1], "%d", &major)
	return major, nil
}

// PgStatStatementsCollector returns a CatalogCollector that queries
// pg_stat_statements, computes deltas between consecutive snapshots,
// and emits a structured payload.
//
// Unlike other collectors that use SELECT * (and rely on the Scanner to
// handle version differences), pg_stat_statements requires explicit column
// lists to avoid transferring potentially huge query text over the wire.
// This means the query must be version-aware. The collector re-checks the
// PG major version on each tick and rebuilds the query if it changes
// (e.g. PG upgrade while the agent is running), clearing the previous
// snapshot since deltas across version boundaries are meaningless.
func PgStatStatementsCollector(
	pool *pgxpool.Pool,
	prepareCtx PrepareCtx,
	includeQueries bool,
	maxQueryTextLength int,
	initialPGVersion int,
) CatalogCollector {
	var prevSnapshot map[string]PgStatStatementsRow
	currentVersion := initialPGVersion
	query := buildPgStatStatementsQuery(includeQueries, maxQueryTextLength, currentVersion)

	return CatalogCollector{
		Name:     PgStatStatementsName,
		Interval: PgStatStatementsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}

			// Re-check PG version to detect upgrades while agent is running.
			detectedVersion, err := queryPGMajorVersion(pool, ctx)
			if err != nil {
				return nil, err
			}
			if detectedVersion != currentVersion {
				currentVersion = detectedVersion
				query = buildPgStatStatementsQuery(includeQueries, maxQueryTextLength, currentVersion)
				// Deltas across version boundaries are meaningless.
				prevSnapshot = nil
			}

			rows, err := CollectView[PgStatStatementsRow](pool, ctx, query, "pg_stat_statements")
			if err != nil {
				return nil, err
			}

			currSnapshot := toSnapshot(rows)

			payload := &PgStatStatementsPayload{
				Rows: rows,
			}

			if prevSnapshot != nil {
				deltas, deltaCount := calculateStatementDeltas(prevSnapshot, currSnapshot)
				avgRuntime := calculateAvgRuntime(prevSnapshot, currSnapshot)
				payload.Deltas = deltas
				payload.DeltaCount = deltaCount
				payload.AverageQueryRuntime = avgRuntime
			}

			prevSnapshot = currSnapshot
			return payload, nil
		},
	}
}
