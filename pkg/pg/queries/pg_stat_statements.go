package queries

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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

	// total_time/min_time/max_time/mean_time/stddev_time were renamed to their
	// _exec_time counterparts in extension 1.8 (default in PG 13). PG 12 ships
	// 1.7 and needs the pre-rename column names, aliased to match PgStatStatementsRow.
	if extVersion.GTE(1, 8) {
		cols = append(cols,
			"calls", "total_exec_time", "min_exec_time", "max_exec_time",
			"mean_exec_time", "stddev_exec_time", "rows",
		)
	} else {
		cols = append(cols,
			"calls",
			"total_time AS total_exec_time", "min_time AS min_exec_time", "max_time AS max_exec_time",
			"mean_time AS mean_exec_time", "stddev_time AS stddev_exec_time", "rows",
		)
	}

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

	// plans/total_plan_time/etc. and the wal_* columns were also added in 1.8.
	if extVersion.GTE(1, 8) {
		cols = append(cols,
			"plans", "total_plan_time", "min_plan_time", "max_plan_time",
			"mean_plan_time", "stddev_plan_time",
			"wal_records", "wal_fpi", "wal_bytes",
		)
	} else {
		cols = append(cols,
			"NULL::bigint AS plans", "NULL::double precision AS total_plan_time",
			"NULL::double precision AS min_plan_time", "NULL::double precision AS max_plan_time",
			"NULL::double precision AS mean_plan_time", "NULL::double precision AS stddev_plan_time",
			"NULL::bigint AS wal_records", "NULL::bigint AS wal_fpi", "NULL::bigint AS wal_bytes",
		)
	}

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

	return fmt.Sprintf("SELECT %s\nFROM public.pg_stat_statements\n%s",
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

// sumPtr returns the nil-aware sum of two pointers (nil acts as absent, not 0).
func sumPtr[T ~int64 | ~float64](a, b *T) *T {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	s := *a + *b
	return &s
}

// minPtr / maxPtr return the nil-aware extremum of two pointers.
func minPtr[T ~int64 | ~float64](a, b *T) *T {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *b < *a {
		return b
	}
	return a
}

func maxPtr[T ~int64 | ~float64](a, b *T) *T {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *b > *a {
		return b
	}
	return a
}

// meanPtr recomputes a running mean as total/n. Returns nil if total is absent,
// and a pointer to 0 when n is absent or 0 (matching pg_stat_statements, which
// reports mean_*_time = 0 when the corresponding call/plan count is 0).
func meanPtr(total *DoublePrecision, n *Bigint) *DoublePrecision {
	if total == nil {
		return nil
	}
	var m DoublePrecision
	if n != nil && *n > 0 {
		m = *total / DoublePrecision(*n)
	}
	return &m
}

// stddevAccumulator combines per-group (n, mean, stddev) triples into a single
// pooled population standard deviation using the parallel/Chan variance
// algorithm. m2 is the running sum of squared deviations (= stddev^2 * n);
// pg_stat_statements reports a population stddev (sqrt(m2/n)), verified against
// a live server, so the same divisor is used here.
type stddevAccumulator struct {
	n    float64
	mean float64
	m2   float64
	any  bool
}

// fold merges one group of gN samples with mean gMean and population stddev
// gStddev into the accumulator. Groups with gN == 0 are no-ops, so plan-time
// rows with plans = 0 contribute nothing (correct: they have no distribution).
func (a *stddevAccumulator) fold(gN, gMean, gStddev float64) {
	a.any = true
	if gN <= 0 {
		return
	}
	gM2 := gStddev * gStddev * gN
	if a.n == 0 {
		a.n, a.mean, a.m2 = gN, gMean, gM2
		return
	}
	nAB := a.n + gN
	delta := gMean - a.mean
	a.mean += delta * gN / nAB
	a.m2 += gM2 + delta*delta*a.n*gN/nAB
	a.n = nAB
}

// result returns the pooled population stddev, or nil if no group ever carried
// the field (so an absent column stays absent rather than becoming 0).
func (a *stddevAccumulator) result() *DoublePrecision {
	if !a.any {
		return nil
	}
	var s DoublePrecision
	if a.n > 0 {
		s = DoublePrecision(math.Sqrt(a.m2 / a.n))
	}
	return &s
}

// pgssAccumulator merges all pg_stat_statements rows that share a composite key
// (queryid, userid, dbid) into a single row. The composite key intentionally
// drops the toplevel dimension, so a query executed both top-level and nested
// (track = all) yields multiple rows here. Aggregating them collapses those
// variants so the downstream payload is unique per composite key.
type pgssAccumulator struct {
	row      PgStatStatementsRow
	execDist stddevAccumulator // weighted by calls
	planDist stddevAccumulator // weighted by plans
}

func newPgssAccumulator(r PgStatStatementsRow) *pgssAccumulator {
	a := &pgssAccumulator{row: r}
	a.foldDistributions(r)
	return a
}

// foldDistributions folds a row's exec-time (weighted by calls) and plan-time
// (weighted by plans) distributions into the pooled accumulators. Called once
// per source row, including the first.
func (a *pgssAccumulator) foldDistributions(r PgStatStatementsRow) {
	if r.Calls != nil && r.MeanExecTime != nil && r.StddevExecTime != nil {
		a.execDist.fold(float64(*r.Calls), float64(*r.MeanExecTime), float64(*r.StddevExecTime))
	}
	if r.Plans != nil && r.MeanPlanTime != nil && r.StddevPlanTime != nil {
		a.planDist.fold(float64(*r.Plans), float64(*r.MeanPlanTime), float64(*r.StddevPlanTime))
	}
}

// merge adds a row sharing the composite key into the accumulator: additive
// counters are summed, extrema are min/max'd, the first non-null query text is
// kept, and the exec/plan distributions are folded for an exact pooled stddev.
func (a *pgssAccumulator) merge(r PgStatStatementsRow) {
	a.foldDistributions(r)
	dst := &a.row

	// Keep the first non-null query text; query and query_len move together.
	if dst.Query == nil {
		dst.Query, dst.QueryLen = r.Query, r.QueryLen
	}

	// Additive counters.
	dst.Calls = sumPtr(dst.Calls, r.Calls)
	dst.Rows = sumPtr(dst.Rows, r.Rows)
	dst.Plans = sumPtr(dst.Plans, r.Plans)
	dst.TotalExecTime = sumPtr(dst.TotalExecTime, r.TotalExecTime)
	dst.TotalPlanTime = sumPtr(dst.TotalPlanTime, r.TotalPlanTime)

	dst.SharedBlksHit = sumPtr(dst.SharedBlksHit, r.SharedBlksHit)
	dst.SharedBlksRead = sumPtr(dst.SharedBlksRead, r.SharedBlksRead)
	dst.SharedBlksDirtied = sumPtr(dst.SharedBlksDirtied, r.SharedBlksDirtied)
	dst.SharedBlksWritten = sumPtr(dst.SharedBlksWritten, r.SharedBlksWritten)
	dst.LocalBlksHit = sumPtr(dst.LocalBlksHit, r.LocalBlksHit)
	dst.LocalBlksRead = sumPtr(dst.LocalBlksRead, r.LocalBlksRead)
	dst.LocalBlksDirtied = sumPtr(dst.LocalBlksDirtied, r.LocalBlksDirtied)
	dst.LocalBlksWritten = sumPtr(dst.LocalBlksWritten, r.LocalBlksWritten)
	dst.TempBlksRead = sumPtr(dst.TempBlksRead, r.TempBlksRead)
	dst.TempBlksWritten = sumPtr(dst.TempBlksWritten, r.TempBlksWritten)

	dst.SharedBlkReadTime = sumPtr(dst.SharedBlkReadTime, r.SharedBlkReadTime)
	dst.SharedBlkWriteTime = sumPtr(dst.SharedBlkWriteTime, r.SharedBlkWriteTime)
	dst.LocalBlkReadTime = sumPtr(dst.LocalBlkReadTime, r.LocalBlkReadTime)
	dst.LocalBlkWriteTime = sumPtr(dst.LocalBlkWriteTime, r.LocalBlkWriteTime)
	dst.TempBlkReadTime = sumPtr(dst.TempBlkReadTime, r.TempBlkReadTime)
	dst.TempBlkWriteTime = sumPtr(dst.TempBlkWriteTime, r.TempBlkWriteTime)

	dst.WalRecords = sumPtr(dst.WalRecords, r.WalRecords)
	dst.WalFpi = sumPtr(dst.WalFpi, r.WalFpi)
	dst.WalBytes = sumPtr(dst.WalBytes, r.WalBytes)

	dst.JitFunctions = sumPtr(dst.JitFunctions, r.JitFunctions)
	dst.JitGenerationTime = sumPtr(dst.JitGenerationTime, r.JitGenerationTime)
	dst.JitInliningCount = sumPtr(dst.JitInliningCount, r.JitInliningCount)
	dst.JitInliningTime = sumPtr(dst.JitInliningTime, r.JitInliningTime)
	dst.JitOptimizationCount = sumPtr(dst.JitOptimizationCount, r.JitOptimizationCount)
	dst.JitOptimizationTime = sumPtr(dst.JitOptimizationTime, r.JitOptimizationTime)
	dst.JitEmissionCount = sumPtr(dst.JitEmissionCount, r.JitEmissionCount)
	dst.JitEmissionTime = sumPtr(dst.JitEmissionTime, r.JitEmissionTime)

	// Extrema.
	dst.MinExecTime = minPtr(dst.MinExecTime, r.MinExecTime)
	dst.MaxExecTime = maxPtr(dst.MaxExecTime, r.MaxExecTime)
	dst.MinPlanTime = minPtr(dst.MinPlanTime, r.MinPlanTime)
	dst.MaxPlanTime = maxPtr(dst.MaxPlanTime, r.MaxPlanTime)
}

// finalize materializes the merged row: means are recomputed from the summed
// totals, stddevs from the pooled accumulators, and toplevel is cleared because
// the merged row spans both toplevel and nested executions.
func (a *pgssAccumulator) finalize() PgStatStatementsRow {
	r := a.row
	r.MeanExecTime = meanPtr(r.TotalExecTime, r.Calls)
	r.MeanPlanTime = meanPtr(r.TotalPlanTime, r.Plans)
	r.StddevExecTime = a.execDist.result()
	r.StddevPlanTime = a.planDist.result()
	r.TopLevel = nil
	return r
}

// aggregateRowsByCompositeKey collapses pg_stat_statements rows that share a
// composite key (queryid, userid, dbid) into one row each, guaranteeing the
// output is unique on that key. This is the single point that fixes the
// duplicate-key family of bugs: with track = all the same queryid can appear as
// both a top-level and a nested row, and emitting both produces duplicate
// composite keys downstream (e.g. an INSERT ... ON CONFLICT (db_instance_id,
// query_id) over an unnested array raises CardinalityViolation). Rows missing
// any key component pass through unmerged in input order (they cannot match
// anything and must not collide on a synthetic "0_0_0" key). Insertion order of
// first-seen keys is preserved for deterministic output.
func aggregateRowsByCompositeKey(rows []PgStatStatementsRow) []PgStatStatementsRow {
	order := make([]string, 0, len(rows))
	accs := make(map[string]*pgssAccumulator, len(rows))
	var passthrough []PgStatStatementsRow

	for i := range rows {
		r := rows[i]
		if r.QueryID == nil || r.UserID == nil || r.DbID == nil {
			passthrough = append(passthrough, r)
			continue
		}
		key := compositeKey(&r)
		if acc, ok := accs[key]; ok {
			acc.merge(r)
			continue
		}
		accs[key] = newPgssAccumulator(r)
		order = append(order, key)
	}

	out := make([]PgStatStatementsRow, 0, len(order)+len(passthrough))
	for _, key := range order {
		out = append(out, accs[key].finalize())
	}
	return append(out, passthrough...)
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

// buildPayloadParts walks the curr rows once and produces everything the
// payload needs:
//
//   - rows / deltas: top `limit` curr rows, ranked by the delta's avg exec
//     time (same metric the standalone delta sort previously used); each
//     kept row gets a paired delta when it had a positive
//     (calls, total_exec_time) diff vs prev.
//   - totalDiffs: pre-cap count of rows with a positive diff (the
//     `delta_count` reported to the server).
//   - avgRuntime: total_exec_time / total_calls summed across ALL
//     positive-diff rows in the snapshot — independent of the row/delta
//     cap, so the AQR reflects the whole database, not just kept rows.
//
// Rows with no positive diff (new rows, idle rows, first tick) sort to the
// end with key 0 and fill any remaining slots in stable input order.
func buildPayloadParts(
	curr []PgStatStatementsRow,
	prev map[string]PgStatStatementsRow,
	limit int,
) (
	rows []PgStatStatementsRow,
	deltas []PgStatStatementsDelta,
	totalDiffs int,
	avgRuntime float64,
	nextSnapshot map[string]PgStatStatementsRow,
) {
	// Collapse rows sharing the composite key (toplevel/nested variants of the
	// same queryid) before any delta, snapshot, or AQR work. This guarantees
	// the emitted rows and deltas are unique per composite key and that the
	// snapshot baseline stored for the next tick is the aggregated value, not
	// an arbitrary single variant.
	curr = aggregateRowsByCompositeKey(curr)

	type ranked struct {
		row     PgStatStatementsRow
		delta   *PgStatStatementsDelta
		sortKey float64
	}

	all := make([]ranked, 0, len(curr))
	nextSnapshot = make(map[string]PgStatStatementsRow, len(curr))
	var totalCalls int64
	var totalExecTime float64

	for _, currRow := range curr {
		entry := ranked{row: currRow}

		// Build the next-tick snapshot inline. Skip rows missing any of
		// the composite-key components — they'd collide on "0_0_0" and
		// can't match anything on the next tick anyway.
		hasKey := currRow.QueryID != nil && currRow.UserID != nil && currRow.DbID != nil
		var key string
		if hasKey {
			key = compositeKey(&currRow)
			nextSnapshot[key] = currRow
		}

		if prev != nil && hasKey {
			prevRow, exists := prev[key]
			callsDiff := ptrDiff(zeroPtr(prevRow.Calls, exists), currRow.Calls)
			execDiff := ptrDiff(zeroPtr(prevRow.TotalExecTime, exists), currRow.TotalExecTime)

			if callsDiff != nil && *callsDiff > 0 &&
				execDiff != nil && *execDiff > 0 {
				totalDiffs++
				totalCalls += int64(*callsDiff)
				totalExecTime += float64(*execDiff)
				entry.delta = &PgStatStatementsDelta{
					UserID:        currRow.UserID,
					DbID:          currRow.DbID,
					QueryID:       currRow.QueryID,
					Calls:         callsDiff,
					TotalExecTime: execDiff,
				}
				entry.sortKey = float64(*execDiff) / float64(*callsDiff)
			}
		}

		all = append(all, entry)
	}

	if totalCalls > 0 {
		avgRuntime = totalExecTime / float64(totalCalls)
	} else {
		avgRuntime = 0.0
	}

	sort.SliceStable(all, func(i, j int) bool {
		return all[i].sortKey > all[j].sortKey
	})

	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}

	rows = make([]PgStatStatementsRow, 0, len(all))
	deltas = make([]PgStatStatementsDelta, 0, len(all))
	for _, entry := range all {
		rows = append(rows, entry.row)
		if entry.delta != nil {
			deltas = append(deltas, *entry.delta)
		}
	}
	return rows, deltas, totalDiffs, avgRuntime, nextSnapshot
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

			// Single pass over curr rows: produces capped rows/deltas
			// (ranked by delta avg exec time), the overall AQR computed
			// across the FULL snapshot, and the curr snapshot map reused
			// as prevSnapshot on the next tick.
			outRows, outDeltas, totalDiffs, avgRuntime, currSnapshot := buildPayloadParts(
				rows, prevSnapshot, cfg.DiffLimit,
			)

			payload := &PgStatStatementsPayload{
				CollectedAt: collectedAt,
				Rows:        outRows,
			}

			if prevSnapshot != nil {
				payload.Deltas = outDeltas
				payload.DeltaCount = totalDiffs
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
