package queries

import (
	"math"
	"strings"
	"testing"
)

func ptrOf[T any](v T) *T { return &v }

// mkRow builds a minimal PgStatStatementsRow with the fields needed by
// buildPayloadParts / calculateAvgRuntime: queryid/userid/dbid for
// composite-key matching, and calls/total_exec_time for the delta math.
func mkRow(queryID int64, calls int64, totalExecTime float64) PgStatStatementsRow {
	return PgStatStatementsRow{
		QueryID:       ptrOf(Bigint(queryID)),
		UserID:        ptrOf(Oid(1)),
		DbID:          ptrOf(Oid(1)),
		Calls:         ptrOf(Bigint(calls)),
		TotalExecTime: ptrOf(DoublePrecision(totalExecTime)),
	}
}

func snapshotOf(rows []PgStatStatementsRow) map[string]PgStatStatementsRow {
	m := make(map[string]PgStatStatementsRow, len(rows))
	for _, r := range rows {
		m[compositeKey(&r)] = r
	}
	return m
}

// Three behaviours covered:
//   1. buildPayloadParts caps rows AND deltas to the same limit.
//   2. Every emitted delta has a matching row in the emitted rows slice
//      (i.e. rows is a strict superset, by composite key, of deltas).
//   3. The avg-query-runtime reported by the payload is computed over the
//      FULL snapshot — every query the agent saw — not just the queries
//      that made it past the cap.

func TestSelectTopByRecentActivity_CapsRowsAndDeltas(t *testing.T) {
	prev := snapshotOf([]PgStatStatementsRow{
		mkRow(1, 10, 100.0),
		mkRow(2, 10, 100.0),
		mkRow(3, 10, 100.0),
		mkRow(4, 10, 100.0),
		mkRow(5, 10, 100.0),
	})
	curr := []PgStatStatementsRow{
		mkRow(1, 20, 110.0),  // delta avg   1
		mkRow(2, 20, 300.0),  // delta avg  20
		mkRow(3, 20, 200.0),  // delta avg  10
		mkRow(4, 20, 1100.0), // delta avg 100   <- top
		mkRow(5, 20, 600.0),  // delta avg  50   <- 2nd
	}

	const limit = 2
	rows, deltas, totalDiffs, _, _ := buildPayloadParts(curr, prev, limit)

	if len(rows) != limit {
		t.Fatalf("len(rows) = %d, want %d", len(rows), limit)
	}
	if len(deltas) != limit {
		t.Fatalf("len(deltas) = %d, want %d", len(deltas), limit)
	}
	if totalDiffs != 5 {
		t.Fatalf("totalDiffs = %d, want 5 (pre-cap count of positive diffs)", totalDiffs)
	}
	// Highest-avg-exec-time deltas survive the cap.
	if int64(*rows[0].QueryID) != 4 || int64(*rows[1].QueryID) != 5 {
		t.Fatalf("expected top-2 rows = [4, 5], got [%d, %d]",
			*rows[0].QueryID, *rows[1].QueryID)
	}
}

func TestSelectTopByRecentActivity_EveryDeltaHasMatchingRow(t *testing.T) {
	// Mix of positive-diff rows and unchanged rows; cap below total.
	prev := snapshotOf([]PgStatStatementsRow{
		mkRow(1, 10, 100.0),
		mkRow(2, 10, 100.0),
		mkRow(3, 10, 100.0),
		mkRow(4, 10, 100.0),
		mkRow(5, 10, 100.0),
	})
	curr := []PgStatStatementsRow{
		mkRow(1, 20, 300.0), // delta
		mkRow(2, 20, 200.0), // delta
		mkRow(3, 20, 110.0), // delta
		mkRow(4, 10, 100.0), // no diff
		mkRow(5, 10, 100.0), // no diff
	}

	rows, deltas, _, _, _ := buildPayloadParts(curr, prev, 3)

	rowIDs := map[int64]struct{}{}
	for _, r := range rows {
		rowIDs[int64(*r.QueryID)] = struct{}{}
	}
	for i, d := range deltas {
		if _, ok := rowIDs[int64(*d.QueryID)]; !ok {
			t.Fatalf("deltas[%d] queryid=%d has no matching row in rows (rowIDs=%v)",
				i, *d.QueryID, rowIDs)
		}
	}
}

// The AQR returned by buildPayloadParts must reflect every positive-diff
// query in the snapshot, not just the queries kept after the row/delta cap.
func TestAverageQueryRuntime_IncludesAllQueriesNotJustCapped(t *testing.T) {
	prev := snapshotOf([]PgStatStatementsRow{
		mkRow(1, 0, 0.0),
		mkRow(2, 0, 0.0),
		mkRow(3, 0, 0.0),
	})
	// Total across ALL three queries: 30 calls, 600 ms  =>  AQR = 20.0
	curr := []PgStatStatementsRow{
		mkRow(1, 10, 100.0),
		mkRow(2, 10, 200.0),
		mkRow(3, 10, 300.0),
	}

	// Cap aggressively — only 1 query survives the row/delta cap.
	rows, _, _, avgRuntime, _ := buildPayloadParts(curr, prev, 1)
	if len(rows) != 1 {
		t.Fatalf("precondition: expected cap to leave 1 row, got %d", len(rows))
	}

	const want = 20.0
	if avgRuntime != want {
		t.Fatalf("avgRuntime = %f, want %f (AQR must use full snapshot, not capped rows)",
			avgRuntime, want)
	}
}

// Tests for buildPgStatStatementsQuery cover the per-extension-version column
// gating documented above the function. The historical bug was that the
// agent gated on the PostgreSQL server major version (>=17 -> use
// shared_blk_read_time), but the column rename happened in the extension
// (1.10 -> 1.11). On a server that has been upgraded to PG 17 with the
// extension still pinned at 1.10 (the realistic Amazon RDS default until
// the operator runs ALTER EXTENSION ... UPDATE), the old gate produced a
// query referencing shared_blk_read_time which does not exist in the 1.10
// view, so the collector failed with SQLSTATE 42703. The case for ext=1.10
// below is the precise regression test for that scenario.
func TestBuildPgStatStatementsQuery_ColumnGating(t *testing.T) {
	type expect struct {
		mustContain    []string
		mustNotContain []string
	}
	cases := []struct {
		name string
		v    PgStatStatementsExtVersion
		expect
	}{
		{
			name: "ext_1_7_pg12_pre_rename_total_time_aliased_no_plans_no_wal",
			v:    PgStatStatementsExtVersion{Major: 1, Minor: 7},
			expect: expect{
				mustContain: []string{
					"total_time AS total_exec_time",
					"min_time AS min_exec_time",
					"max_time AS max_exec_time",
					"mean_time AS mean_exec_time",
					"stddev_time AS stddev_exec_time",
					"NULL::bigint AS plans",
					"NULL::bigint AS wal_records",
					"blk_read_time AS shared_blk_read_time",
				},
				mustNotContain: []string{
					", total_exec_time", // bare (unaliased) column must not appear
					"toplevel",
					"jit_functions",
					"temp_blk_read_time",
					"local_blk_read_time",
				},
			},
		},
		{
			name: "ext_1_8_pg13_no_toplevel_no_jit_no_temp_blk_time_blk_read_time_aliased",
			v:    PgStatStatementsExtVersion{Major: 1, Minor: 8},
			expect: expect{
				mustContain: []string{
					"blk_read_time AS shared_blk_read_time",
					"blk_write_time AS shared_blk_write_time",
				},
				mustNotContain: []string{
					"toplevel",
					"jit_functions",
					"temp_blk_read_time",
					", shared_blk_read_time",
					"local_blk_read_time",
				},
			},
		},
		{
			name: "ext_1_9_pg14_adds_toplevel",
			v:    PgStatStatementsExtVersion{Major: 1, Minor: 9},
			expect: expect{
				mustContain: []string{
					"blk_read_time AS shared_blk_read_time",
					"toplevel",
				},
				mustNotContain: []string{
					"jit_functions",
					"temp_blk_read_time",
					"local_blk_read_time",
				},
			},
		},
		{
			name: "ext_1_10_pg15_adds_temp_blk_time_and_jit",
			v:    PgStatStatementsExtVersion{Major: 1, Minor: 10},
			expect: expect{
				mustContain: []string{
					"blk_read_time AS shared_blk_read_time",
					"toplevel",
					"temp_blk_read_time",
					"temp_blk_write_time",
					"jit_functions",
					"jit_emission_time",
				},
				mustNotContain: []string{
					", shared_blk_read_time",
					"local_blk_read_time",
				},
			},
		},
		{
			name: "ext_1_10_on_pg17_server_regression_for_RDS_upgrade_path",
			v:    PgStatStatementsExtVersion{Major: 1, Minor: 10},
			expect: expect{
				// This is the realistic Amazon RDS state after a PG 16 -> 17
				// server upgrade without ALTER EXTENSION ... UPDATE: the
				// query MUST still reference blk_read_time (aliased), not
				// shared_blk_read_time, because the 1.10 view doesn't have
				// the new column.
				mustContain: []string{
					"blk_read_time AS shared_blk_read_time",
				},
				mustNotContain: []string{
					", shared_blk_read_time",
					"local_blk_read_time",
				},
			},
		},
		{
			name: "ext_1_11_pg17_renames_to_shared_blk_time_adds_local_blk_time",
			v:    PgStatStatementsExtVersion{Major: 1, Minor: 11},
			expect: expect{
				mustContain: []string{
					"shared_blk_read_time",
					"shared_blk_write_time",
					"local_blk_read_time",
					"local_blk_write_time",
					"toplevel",
					"temp_blk_read_time",
					"jit_functions",
				},
				mustNotContain: []string{
					"blk_read_time AS shared_blk_read_time",
					"blk_write_time AS shared_blk_write_time",
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := buildPgStatStatementsQuery(true, 4096, tc.v)
			for _, s := range tc.mustContain {
				if !strings.Contains(q, s) {
					t.Errorf("expected query to contain %q\nquery:\n%s", s, q)
				}
			}
			for _, s := range tc.mustNotContain {
				if strings.Contains(q, s) {
					t.Errorf("expected query NOT to contain %q\nquery:\n%s", s, q)
				}
			}
		})
	}
}

func TestPgStatStatementsExtVersion_GTE(t *testing.T) {
	cases := []struct {
		v          PgStatStatementsExtVersion
		major      int
		minor      int
		wantResult bool
	}{
		{PgStatStatementsExtVersion{1, 10}, 1, 10, true},
		{PgStatStatementsExtVersion{1, 10}, 1, 11, false},
		{PgStatStatementsExtVersion{1, 11}, 1, 11, true},
		{PgStatStatementsExtVersion{1, 11}, 1, 10, true},
		{PgStatStatementsExtVersion{2, 0}, 1, 99, true},
		{PgStatStatementsExtVersion{0, 99}, 1, 0, false},
	}
	for _, c := range cases {
		got := c.v.GTE(c.major, c.minor)
		if got != c.wantResult {
			t.Errorf("(%d.%d).GTE(%d,%d) = %v, want %v",
				c.v.Major, c.v.Minor, c.major, c.minor, got, c.wantResult)
		}
	}
}

// ---------------------------------------------------------------------------
// Composite-key aggregation (toplevel/nested collapse).
//
// pg_stat_statements with track = all reports the same queryid as two rows
// (toplevel = true and toplevel = false) when a query runs both directly and
// nested. The agent's composite key (queryid, userid, dbid) drops the toplevel
// dimension, so those rows must be aggregated into one before anything emits
// them, or the platform's INSERT ... ON CONFLICT (db_instance_id, query_id)
// over an unnested array raises CardinalityViolation. The merge semantics were
// verified against a live PostgreSQL 17 server: top-level and nested calls for
// one queryid are disjoint invocation sets, so calls/total_exec_time SUM; pg
// reports a population stddev; and a parent's time being inclusive of children
// only ever spans DIFFERENT queryids (never merged here).
// ---------------------------------------------------------------------------

const aggEpsilon = 1e-9

func approxEq(a, b float64) bool { return math.Abs(a-b) <= aggEpsilon }

// variant builds a richer row for one (queryid, toplevel) combination, with the
// distribution fields needed to exercise mean/stddev/min/max merging.
type variant struct {
	queryID       int64
	toplevel      bool
	calls         int64
	totalExecTime float64
	minExecTime   float64
	maxExecTime   float64
	meanExecTime  float64
	stddevExec    float64
	plans         int64
	totalPlanTime float64
	minPlanTime   float64
	maxPlanTime   float64
	meanPlanTime  float64
	stddevPlan    float64
	sharedBlksHit int64
	walBytes      int64
}

func (v variant) row() PgStatStatementsRow {
	return PgStatStatementsRow{
		QueryID:        ptrOf(Bigint(v.queryID)),
		UserID:         ptrOf(Oid(1)),
		DbID:           ptrOf(Oid(1)),
		TopLevel:       ptrOf(Boolean(v.toplevel)),
		Calls:          ptrOf(Bigint(v.calls)),
		TotalExecTime:  ptrOf(DoublePrecision(v.totalExecTime)),
		MinExecTime:    ptrOf(DoublePrecision(v.minExecTime)),
		MaxExecTime:    ptrOf(DoublePrecision(v.maxExecTime)),
		MeanExecTime:   ptrOf(DoublePrecision(v.meanExecTime)),
		StddevExecTime: ptrOf(DoublePrecision(v.stddevExec)),
		Plans:          ptrOf(Bigint(v.plans)),
		TotalPlanTime:  ptrOf(DoublePrecision(v.totalPlanTime)),
		MinPlanTime:    ptrOf(DoublePrecision(v.minPlanTime)),
		MaxPlanTime:    ptrOf(DoublePrecision(v.maxPlanTime)),
		MeanPlanTime:   ptrOf(DoublePrecision(v.meanPlanTime)),
		StddevPlanTime: ptrOf(DoublePrecision(v.stddevPlan)),
		SharedBlksHit:  ptrOf(Bigint(v.sharedBlksHit)),
		WalBytes:       ptrOf(Bigint(v.walBytes)),
	}
}

func deref[T any](p *T) T {
	if p == nil {
		var z T
		return z
	}
	return *p
}

// uniqueByCompositeKey fails if any composite key appears more than once.
func uniqueByCompositeKey(t *testing.T, rows []PgStatStatementsRow) {
	t.Helper()
	seen := make(map[string]int)
	for i := range rows {
		seen[compositeKey(&rows[i])]++
	}
	for k, n := range seen {
		if n > 1 {
			t.Fatalf("composite key %q appears %d times — not unique", k, n)
		}
	}
}

// TestAggregate_CollapsesToplevelVariants is the direct regression for the
// CardinalityViolation: the same queryid as toplevel=true and toplevel=false
// must collapse to one row with summed calls and total_exec_time.
func TestAggregate_CollapsesToplevelVariants(t *testing.T) {
	in := []PgStatStatementsRow{
		variant{queryID: 42, toplevel: true, calls: 5, totalExecTime: 100}.row(),
		variant{queryID: 42, toplevel: false, calls: 3, totalExecTime: 50}.row(),
	}
	out := aggregateRowsByCompositeKey(in)

	if len(out) != 1 {
		t.Fatalf("expected 1 aggregated row, got %d", len(out))
	}
	uniqueByCompositeKey(t, out)
	if got := int64(deref(out[0].Calls)); got != 8 {
		t.Errorf("calls = %d, want 8", got)
	}
	if got := float64(deref(out[0].TotalExecTime)); !approxEq(got, 150) {
		t.Errorf("total_exec_time = %f, want 150", got)
	}
	if out[0].TopLevel != nil {
		t.Errorf("toplevel must be cleared on a merged row, got %v", *out[0].TopLevel)
	}
}

// TestAggregate_FullFieldMerge checks every aggregation rule in one go:
// additive sum, min/max extrema, mean recomputed from totals, and pooled
// population stddev for both exec-time (weighted by calls) and plan-time
// (weighted by plans). Group raw samples are chosen so the pooled stddev has a
// closed-form value: exec {10,20} ∪ {30} -> mean 20, pop stddev sqrt(200/3).
func TestAggregate_FullFieldMerge(t *testing.T) {
	a := variant{
		queryID: 7, toplevel: true,
		calls: 2, totalExecTime: 30, minExecTime: 10, maxExecTime: 20, meanExecTime: 15, stddevExec: 5,
		plans: 2, totalPlanTime: 30, minPlanTime: 10, maxPlanTime: 20, meanPlanTime: 15, stddevPlan: 5,
		sharedBlksHit: 5, walBytes: 100,
	}
	b := variant{
		queryID: 7, toplevel: false,
		calls: 1, totalExecTime: 30, minExecTime: 30, maxExecTime: 30, meanExecTime: 30, stddevExec: 0,
		plans: 1, totalPlanTime: 30, minPlanTime: 30, maxPlanTime: 30, meanPlanTime: 30, stddevPlan: 0,
		sharedBlksHit: 7, walBytes: 200,
	}
	out := aggregateRowsByCompositeKey([]PgStatStatementsRow{a.row(), b.row()})
	if len(out) != 1 {
		t.Fatalf("expected 1 row, got %d", len(out))
	}
	r := out[0]

	wantPooled := math.Sqrt(200.0 / 3.0) // ≈ 8.16497

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"calls", float64(deref(r.Calls)), 3},
		{"total_exec_time", float64(deref(r.TotalExecTime)), 60},
		{"mean_exec_time (total/calls)", float64(deref(r.MeanExecTime)), 20},
		{"min_exec_time", float64(deref(r.MinExecTime)), 10},
		{"max_exec_time", float64(deref(r.MaxExecTime)), 30},
		{"stddev_exec_time (pooled)", float64(deref(r.StddevExecTime)), wantPooled},
		{"plans", float64(deref(r.Plans)), 3},
		{"total_plan_time", float64(deref(r.TotalPlanTime)), 60},
		{"mean_plan_time (total/plans)", float64(deref(r.MeanPlanTime)), 20},
		{"min_plan_time", float64(deref(r.MinPlanTime)), 10},
		{"max_plan_time", float64(deref(r.MaxPlanTime)), 30},
		{"stddev_plan_time (pooled)", float64(deref(r.StddevPlanTime)), wantPooled},
		{"shared_blks_hit", float64(deref(r.SharedBlksHit)), 12},
		{"wal_bytes", float64(deref(r.WalBytes)), 300},
	}
	for _, c := range checks {
		if !approxEq(c.got, c.want) {
			t.Errorf("%s = %g, want %g", c.name, c.got, c.want)
		}
	}
}

// TestAggregate_PooledStddevMatchesRawSamples verifies the pooled stddev against
// the stddev computed directly from the underlying raw samples, for an
// asymmetric split (different group sizes and means, exercising the
// between-group term).
func TestAggregate_PooledStddevMatchesRawSamples(t *testing.T) {
	// Raw samples split across two toplevel variants of one queryid.
	groupA := []float64{10, 10, 10} // n=3, mean=10, pop stddev 0
	groupB := []float64{30}         // n=1, mean=30, pop stddev 0
	all := append(append([]float64{}, groupA...), groupB...)

	popStddev := func(xs []float64) float64 {
		var sum float64
		for _, x := range xs {
			sum += x
		}
		mean := sum / float64(len(xs))
		var ss float64
		for _, x := range xs {
			ss += (x - mean) * (x - mean)
		}
		return math.Sqrt(ss / float64(len(xs)))
	}

	mk := func(top bool, xs []float64) PgStatStatementsRow {
		v := variant{queryID: 99, toplevel: top, calls: int64(len(xs))}
		var sum float64
		for _, x := range xs {
			sum += x
		}
		v.totalExecTime = sum
		v.meanExecTime = sum / float64(len(xs))
		v.stddevExec = popStddev(xs)
		// plan fields mirror exec so plan-weighted pooling is exercised too.
		v.plans = int64(len(xs))
		v.totalPlanTime = sum
		v.meanPlanTime = v.meanExecTime
		v.stddevPlan = v.stddevExec
		return v.row()
	}

	out := aggregateRowsByCompositeKey([]PgStatStatementsRow{mk(true, groupA), mk(false, groupB)})
	if len(out) != 1 {
		t.Fatalf("expected 1 row, got %d", len(out))
	}
	want := popStddev(all) // sqrt(200/3) ≈ 8.16497
	if got := float64(deref(out[0].StddevExecTime)); !approxEq(got, want) {
		t.Errorf("pooled stddev_exec_time = %g, want %g (from raw samples)", got, want)
	}
	if got := float64(deref(out[0].StddevPlanTime)); !approxEq(got, want) {
		t.Errorf("pooled stddev_plan_time = %g, want %g (from raw samples)", got, want)
	}
}

// TestAggregate_ZeroPlansContributeNothing checks that plan-time pooling ignores
// rows with plans = 0 (no planning distribution) while still summing exec-time.
func TestAggregate_ZeroPlansContributeNothing(t *testing.T) {
	withPlans := variant{
		queryID: 3, toplevel: true,
		calls: 4, totalExecTime: 40, meanExecTime: 10, stddevExec: 2,
		plans: 4, totalPlanTime: 8, meanPlanTime: 2, stddevPlan: 1,
	}
	noPlans := variant{
		queryID: 3, toplevel: false,
		calls: 4, totalExecTime: 40, meanExecTime: 10, stddevExec: 2,
		plans: 0, totalPlanTime: 0, meanPlanTime: 0, stddevPlan: 0,
	}
	out := aggregateRowsByCompositeKey([]PgStatStatementsRow{withPlans.row(), noPlans.row()})
	if len(out) != 1 {
		t.Fatalf("expected 1 row, got %d", len(out))
	}
	r := out[0]
	if got := int64(deref(r.Plans)); got != 4 {
		t.Errorf("plans = %d, want 4", got)
	}
	// Only the withPlans group has a distribution, so the pooled stddev equals
	// that group's stddev unchanged.
	if got := float64(deref(r.StddevPlanTime)); !approxEq(got, 1) {
		t.Errorf("stddev_plan_time = %g, want 1 (zero-plan row is a no-op)", got)
	}
	if got := float64(deref(r.MeanPlanTime)); !approxEq(got, 2) {
		t.Errorf("mean_plan_time = %g, want 2 (8/4)", got)
	}
}

// TestAggregate_UniquePerKeyAcrossManyVariants feeds an interleaved mix and
// asserts the output collapses to exactly one row per composite key, preserving
// first-seen order, with correct per-key sums.
func TestAggregate_UniquePerKeyAcrossManyVariants(t *testing.T) {
	in := []PgStatStatementsRow{
		variant{queryID: 1, toplevel: true, calls: 1, totalExecTime: 10}.row(),
		variant{queryID: 2, toplevel: true, calls: 2, totalExecTime: 20}.row(),
		variant{queryID: 1, toplevel: false, calls: 3, totalExecTime: 30}.row(),
		variant{queryID: 3, toplevel: true, calls: 4, totalExecTime: 40}.row(),
		variant{queryID: 2, toplevel: false, calls: 5, totalExecTime: 50}.row(),
		variant{queryID: 1, toplevel: false, calls: 6, totalExecTime: 60}.row(), // 3rd variant of qid 1
	}
	out := aggregateRowsByCompositeKey(in)

	if len(out) != 3 {
		t.Fatalf("expected 3 unique rows (qids 1,2,3), got %d", len(out))
	}
	uniqueByCompositeKey(t, out)

	// First-seen order preserved: qid 1, 2, 3.
	wantOrder := []int64{1, 2, 3}
	for i, want := range wantOrder {
		if got := int64(deref(out[i].QueryID)); got != want {
			t.Errorf("out[%d] queryid = %d, want %d (first-seen order)", i, got, want)
		}
	}
	byQID := map[int64]PgStatStatementsRow{}
	for _, r := range out {
		byQID[int64(deref(r.QueryID))] = r
	}
	if got := int64(deref(byQID[1].Calls)); got != 1+3+6 {
		t.Errorf("qid 1 calls = %d, want 10", got)
	}
	if got := int64(deref(byQID[2].Calls)); got != 2+5 {
		t.Errorf("qid 2 calls = %d, want 7", got)
	}
	if got := int64(deref(byQID[3].Calls)); got != 4 {
		t.Errorf("qid 3 calls = %d, want 4", got)
	}
}

// TestAggregate_PassthroughUnkeyedRows verifies rows missing any composite-key
// component are passed through unmerged (never collapsed onto a synthetic key)
// and appended after the keyed rows.
func TestAggregate_PassthroughUnkeyedRows(t *testing.T) {
	noQID := PgStatStatementsRow{UserID: ptrOf(Oid(1)), DbID: ptrOf(Oid(1)), Calls: ptrOf(Bigint(9))}
	noDB := PgStatStatementsRow{QueryID: ptrOf(Bigint(5)), UserID: ptrOf(Oid(1)), Calls: ptrOf(Bigint(8))}
	keyed := variant{queryID: 5, toplevel: true, calls: 1, totalExecTime: 10}.row()

	out := aggregateRowsByCompositeKey([]PgStatStatementsRow{noQID, keyed, noDB})
	if len(out) != 3 {
		t.Fatalf("expected 3 rows (1 keyed + 2 passthrough), got %d", len(out))
	}
	// Keyed row first, then passthrough in input order.
	if out[0].QueryID == nil || int64(*out[0].QueryID) != 5 || out[0].TopLevel != nil {
		t.Errorf("expected merged keyed row (qid 5) first, got %+v", out[0])
	}
	if int64(deref(out[1].Calls)) != 9 || int64(deref(out[2].Calls)) != 8 {
		t.Errorf("passthrough rows reordered or merged: %d, %d", deref(out[1].Calls), deref(out[2].Calls))
	}
}

// TestBuildPayloadParts_DedupsDeltasAndAggregatesBaseline ties the fix to the
// delta path across two ticks. It proves both:
//  1. duplicate toplevel variants emit exactly ONE delta per composite key
//     (the CardinalityViolation regression), and
//  2. the snapshot baseline stored for the next tick is the AGGREGATED sum, so
//     the next delta is computed against calls=9, not an arbitrary single
//     variant (the latent overwrite bug at nextSnapshot[key] = currRow).
func TestBuildPayloadParts_DedupsDeltasAndAggregatesBaseline(t *testing.T) {
	const limit = 500

	// Tick 1: establish baseline. Two variants of one queryid -> aggregate 9 calls.
	tick1 := []PgStatStatementsRow{
		variant{queryID: 42, toplevel: true, calls: 5, totalExecTime: 100}.row(),
		variant{queryID: 42, toplevel: false, calls: 4, totalExecTime: 80}.row(),
	}
	_, deltas1, _, _, snap1 := buildPayloadParts(tick1, nil, limit)
	if len(deltas1) != 0 {
		t.Fatalf("first tick must have no deltas (no prev), got %d", len(deltas1))
	}
	if len(snap1) != 1 {
		t.Fatalf("snapshot must hold 1 aggregated entry, got %d", len(snap1))
	}
	var baseline PgStatStatementsRow
	for _, r := range snap1 {
		baseline = r
	}
	if got := int64(deref(baseline.Calls)); got != 9 {
		t.Fatalf("baseline calls = %d, want 9 (aggregated 5+4, not a single variant)", got)
	}

	// Tick 2: same queryid grows. Aggregated 13 calls, 240 total.
	tick2 := []PgStatStatementsRow{
		variant{queryID: 42, toplevel: true, calls: 7, totalExecTime: 130}.row(),
		variant{queryID: 42, toplevel: false, calls: 6, totalExecTime: 110}.row(),
	}
	rows2, deltas2, totalDiffs, _, _ := buildPayloadParts(tick2, snap1, limit)

	uniqueByCompositeKey(t, rows2)
	if len(deltas2) != 1 {
		t.Fatalf("expected exactly 1 delta for the single composite key, got %d", len(deltas2))
	}
	if totalDiffs != 1 {
		t.Errorf("delta_count = %d, want 1 (per composite key, not per variant)", totalDiffs)
	}
	// Delta is aggregated-curr (13/240) minus aggregated-baseline (9/180).
	if got := int64(deref(deltas2[0].Calls)); got != 4 {
		t.Errorf("delta calls = %d, want 4 (13-9). A wrong baseline would give 8 or 9", got)
	}
	if got := float64(deref(deltas2[0].TotalExecTime)); !approxEq(got, 60) {
		t.Errorf("delta total_exec_time = %g, want 60 (240-180)", got)
	}
}
