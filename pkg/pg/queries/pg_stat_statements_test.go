package queries

import (
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
