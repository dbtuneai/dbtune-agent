package queries

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatStatementsName     = "pg_stat_statements"
	PgStatStatementsInterval = 5 * time.Second
)

// pgStatStatementsFilter is the WHERE clause shared by all pg_stat_statements queries.
var pgStatStatementsFilter = fmt.Sprintf(`
WHERE NOT starts_with(query, '%s')
  AND query !~* '^\s*(BEGIN|COMMIT|ROLLBACK|SET |SHOW |SELECT (pg_|\$1$|version\s*\(\s*\)))\s*;?\s*$'
`, utils.DBtuneQueryPrefix)

// PgStatStatementsDiffLimit is the max number of delta entries to include.
const PgStatStatementsDiffLimit = 500

// PgStatStatementsConfig holds configuration for the pg_stat_statements collector.
type PgStatStatementsConfig struct {
	DiffLimit          int  `config:"diff_limit" default:"500" min:"0" max:"500"`
	IncludeQueries     bool `config:"include_queries" default:"true"`
	MaxQueryTextLength int  `config:"max_query_text_length" default:"8192" min:"0" max:"8192"`
}

// PgStatStatementsRow represents a single row from pg_stat_statements.
type PgStatStatementsRow struct {
	// Identifiers
	UserID  *Oid    `json:"userid" db:"userid"`
	DbID    *Oid    `json:"dbid" db:"dbid"`
	QueryID *Bigint `json:"queryid" db:"queryid"`

	// Query text
	Query    *Text   `json:"query,omitempty" db:"query"`
	QueryLen *Bigint `json:"query_len,omitempty" db:"query_len"`

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

	// Block I/O timing — PG17 renamed blk_read/write_time to shared_blk_read/write_time.
	SharedBlkReadTime  *DoublePrecision `json:"shared_blk_read_time" db:"shared_blk_read_time"`
	SharedBlkWriteTime *DoublePrecision `json:"shared_blk_write_time" db:"shared_blk_write_time"`

	// PG17+: local block I/O timing
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
	WalBytes       *Bigint          `json:"wal_bytes" db:"wal_bytes"`

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
type PgStatStatementsDelta struct {
	UserID  *Oid    `json:"userid" db:"userid"`
	DbID    *Oid    `json:"dbid" db:"dbid"`
	QueryID *Bigint `json:"queryid" db:"queryid"`

	Calls         *Bigint          `json:"calls" db:"calls"`
	TotalExecTime *DoublePrecision `json:"total_exec_time" db:"total_exec_time"`
}

// PgStatStatementsPayload is the JSON body POSTed to /api/v1/agent/pg_stat_statements.
type PgStatStatementsPayload struct {
	CollectedAt         time.Time               `json:"collected_at"`
	Rows                []PgStatStatementsRow   `json:"rows"`
	Deltas              []PgStatStatementsDelta `json:"deltas,omitempty"`
	DeltaCount          int                     `json:"delta_count"`
	AverageQueryRuntime float64                 `json:"average_query_runtime"`
}

// PgStatStatementsExtVersion is a parsed pg_stat_statements extension version
// (e.g. extversion '1.10' -> {Major:1, Minor:10}). The available column set is
// determined by this extension version, not by the PostgreSQL server major
// version: a server that has been upgraded (e.g. PG 16 -> 17) keeps the
// previously-installed extension version until ALTER EXTENSION ... UPDATE is
// run, so the two move independently. Managed services such as Amazon RDS
// regularly upgrade the server but leave existing extensions at their old
// version, producing the realistic "new server / old extension" combination.
//
// References:
//   - https://www.postgresql.org/docs/current/pgstatstatements.html
//   - https://www.postgresql.org/docs/current/sql-alterextension.html (ALTER EXTENSION ... UPDATE)
type PgStatStatementsExtVersion struct {
	Major int
	Minor int
}

// GTE reports whether v is at least major.minor.
func (v PgStatStatementsExtVersion) GTE(major, minor int) bool {
	if v.Major != major {
		return v.Major > major
	}
	return v.Minor >= minor
}

// buildPgStatStatementsQuery returns an extension-version-specific query for
// pg_stat_statements. Column availability follows the extension changelog;
// the version-to-PG mapping below is the default_version recorded in the
// pg_stat_statements.control file for each PostgreSQL stable branch (the
// canonical source of truth for which extension version a fresh PG ships).
//
//   - 1.8  (default in PG 13): split total_time / min_time / max_time /
//     mean_time / stddev_time into _exec_ / _plan_ counterparts.
//     https://github.com/postgres/postgres/blob/REL_13_STABLE/contrib/pg_stat_statements/pg_stat_statements.control
//   - 1.9  (default in PG 14): adds the toplevel column.
//     https://github.com/postgres/postgres/blob/REL_14_STABLE/contrib/pg_stat_statements/pg_stat_statements.control
//     https://www.postgresql.org/docs/release/14.0/ (separate top/nested tracking)
//   - 1.10 (default in PG 15 and PG 16): adds temp_blk_read_time /
//     temp_blk_write_time and the jit_* columns (jit_functions,
//     jit_generation_time, jit_inlining_count, jit_inlining_time,
//     jit_optimization_count, jit_optimization_time, jit_emission_count,
//     jit_emission_time).
//     https://github.com/postgres/postgres/blob/REL_15_STABLE/contrib/pg_stat_statements/pg_stat_statements.control
//     https://github.com/postgres/postgres/blob/REL_16_STABLE/contrib/pg_stat_statements/pg_stat_statements.control
//     https://www.postgresql.org/docs/release/15.0/ (temp file I/O + JIT counters)
//   - 1.11 (default in PG 17): renames blk_read_time / blk_write_time to
//     shared_blk_read_time / shared_blk_write_time, and adds
//     local_blk_read_time / local_blk_write_time.
//     https://github.com/postgres/postgres/blob/REL_17_STABLE/contrib/pg_stat_statements/pg_stat_statements.control
//     https://www.postgresql.org/docs/release/17.0/ (E.10.3.11.1 pg_stat_statements)
//
// Because PG 16 shipped 1.10 by default, an Amazon RDS instance upgraded from
// PG 16 to PG 17 keeps the extension at 1.10 until the operator explicitly
// runs ALTER EXTENSION pg_stat_statements UPDATE — that's the realistic
// "new server / old extension" combination this gating must handle.
func buildPgStatStatementsQuery(includeQueries bool, maxQueryTextLength int, extVersion PgStatStatementsExtVersion) string {
	var cols []string

	cols = append(cols, "userid", "dbid", "queryid")

	if !includeQueries {
		cols = append(cols, "NULL::text AS query", "NULL::bigint AS query_len")
	} else {
		cols = append(cols,
			fmt.Sprintf("LEFT(query, %d) AS query", maxQueryTextLength),
			"LENGTH(query) AS query_len",
		)
	}

	cols = append(cols,
		"calls", "total_exec_time", "min_exec_time", "max_exec_time",
		"mean_exec_time", "stddev_exec_time", "rows",
	)

	cols = append(cols,
		"shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written",
		"local_blks_hit", "local_blks_read", "local_blks_dirtied", "local_blks_written",
		"temp_blks_read", "temp_blks_written",
	)

	if extVersion.GTE(1, 11) {
		cols = append(cols, "shared_blk_read_time", "shared_blk_write_time")
		cols = append(cols, "local_blk_read_time", "local_blk_write_time")
	} else {
		cols = append(cols,
			"blk_read_time AS shared_blk_read_time",
			"blk_write_time AS shared_blk_write_time",
		)
	}

	cols = append(cols,
		"plans", "total_plan_time", "min_plan_time", "max_plan_time",
		"mean_plan_time", "stddev_plan_time",
		"wal_records", "wal_fpi", "wal_bytes",
	)

	if extVersion.GTE(1, 9) {
		cols = append(cols, "toplevel")
	}

	if extVersion.GTE(1, 10) {
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

// ptrDiff returns *curr - *prev if both are non-nil and the result is non-negative.
func ptrDiff[T ~int64 | ~float64](prev, curr *T) *T {
	if prev == nil || curr == nil {
		return nil
	}
	d := *curr - *prev
	if d < 0 {
		return nil
	}
	return &d
}

// zeroPtr returns a pointer to zero if the row didn't exist in prev,
// or the actual value if it did.
func zeroPtr[T ~int64 | ~float64](val *T, exists bool) *T {
	if !exists {
		zero := T(0)
		return &zero
	}
	return val
}

func calculateStatementDeltas(prev, curr map[string]PgStatStatementsRow, diffLimit int) ([]PgStatStatementsDelta, int) {
	diffs := make([]PgStatStatementsDelta, 0, len(curr))
	totalDiffs := 0

	for key, currRow := range curr {
		prevRow, exists := prev[key]

		callsDiff := ptrDiff(zeroPtr(prevRow.Calls, exists), currRow.Calls)
		execTimeDiff := ptrDiff(zeroPtr(prevRow.TotalExecTime, exists), currRow.TotalExecTime)

		if callsDiff == nil || *callsDiff <= 0 ||
			execTimeDiff == nil || *execTimeDiff <= 0 {
			continue
		}

		totalDiffs++

		diffs = append(diffs, PgStatStatementsDelta{
			UserID:        currRow.UserID,
			DbID:          currRow.DbID,
			QueryID:       currRow.QueryID,
			Calls:         callsDiff,
			TotalExecTime: execTimeDiff,
		})
	}

	sort.Slice(diffs, func(i, j int) bool {
		avgI := float64(*diffs[i].TotalExecTime) / float64(*diffs[i].Calls)
		avgJ := float64(*diffs[j].TotalExecTime) / float64(*diffs[j].Calls)
		return avgI > avgJ
	})

	if len(diffs) > diffLimit {
		diffs = diffs[:diffLimit]
	}

	return diffs, totalDiffs
}

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

// pgStatStatementsExtVersionRegex extracts the major.minor pair from a
// pg_extension.extversion value such as "1.10".
var pgStatStatementsExtVersionRegex = regexp.MustCompile(`^(\d+)\.(\d+)`)

const pgStatStatementsExtVersionQuery = `SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_statements'`

func queryPgStatStatementsExtVersion(pool *pgxpool.Pool, ctx context.Context) (PgStatStatementsExtVersion, error) {
	var s string
	err := utils.QueryRowWithPrefix(pool, ctx, pgStatStatementsExtVersionQuery).Scan(&s)
	if err != nil {
		return PgStatStatementsExtVersion{}, fmt.Errorf("failed to query pg_stat_statements extension version: %w", err)
	}
	m := pgStatStatementsExtVersionRegex.FindStringSubmatch(s)
	if len(m) < 3 {
		return PgStatStatementsExtVersion{}, fmt.Errorf("could not parse pg_stat_statements extension version from %q", s)
	}
	major, err := strconv.Atoi(m[1])
	if err != nil {
		return PgStatStatementsExtVersion{}, fmt.Errorf("could not parse major version from %q: %w", s, err)
	}
	minor, err := strconv.Atoi(m[2])
	if err != nil {
		return PgStatStatementsExtVersion{}, fmt.Errorf("could not parse minor version from %q: %w", s, err)
	}
	return PgStatStatementsExtVersion{Major: major, Minor: minor}, nil
}

// PgStatStatementsCollector returns a CatalogCollector that queries
// pg_stat_statements, computes deltas between consecutive snapshots,
// and emits a structured payload. The query is rebuilt whenever the
// detected pg_stat_statements extension version changes (e.g. after
// ALTER EXTENSION pg_stat_statements UPDATE).
func PgStatStatementsCollector(
	pool *pgxpool.Pool,
	prepareCtx PrepareCtx,
	cfg PgStatStatementsConfig,
) CatalogCollector {
	var prevSnapshot map[string]PgStatStatementsRow
	var currentExtVersion PgStatStatementsExtVersion
	var query string
	scanner := pgxutil.NewScanner[PgStatStatementsRow]()

	return CatalogCollector{
		Name:     PgStatStatementsName,
		Interval: PgStatStatementsInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}

			collectedAt := time.Now().UTC()
			detectedExtVersion, err := queryPgStatStatementsExtVersion(pool, ctx)
			if err != nil {
				return nil, err
			}
			if detectedExtVersion != currentExtVersion {
				currentExtVersion = detectedExtVersion
				query = buildPgStatStatementsQuery(cfg.IncludeQueries, cfg.MaxQueryTextLength, currentExtVersion)
				prevSnapshot = nil
			}

			querier := func() (pgx.Rows, error) {
				return utils.QueryWithPrefix(pool, ctx, query)
			}
			rows, err := CollectView(querier, "pg_stat_statements", scanner)
			if err != nil {
				return nil, err
			}

			currSnapshot := toSnapshot(rows)

			payload := &PgStatStatementsPayload{
				CollectedAt: collectedAt,
				Rows:        rows,
			}

			if prevSnapshot != nil {
				deltas, deltaCount := calculateStatementDeltas(prevSnapshot, currSnapshot, cfg.DiffLimit)
				avgRuntime := calculateAvgRuntime(prevSnapshot, currSnapshot)
				payload.Deltas = deltas
				payload.DeltaCount = deltaCount
				payload.AverageQueryRuntime = avgRuntime
			}

			prevSnapshot = currSnapshot

			data, err := json.Marshal(payload)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", PgStatStatementsName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
