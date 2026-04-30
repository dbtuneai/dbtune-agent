package queries

import (
	"strings"
	"testing"
)

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
