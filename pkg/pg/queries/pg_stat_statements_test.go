package queries

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers ---

func bigintPtr(v int64) *Bigint            { b := Bigint(v); return &b }
func doublePtr(v float64) *DoublePrecision { d := DoublePrecision(v); return &d }
func oidPtr(v uint32) *Oid                 { o := Oid(v); return &o }
func textPtr(v string) *Text               { t := Text(v); return &t }

func fakeStatementsRow(qid int64, uid, did uint32, calls int64, execTime float64, rows int64) PgStatStatementsRow {
	return PgStatStatementsRow{
		UserID:        oidPtr(uid),
		DbID:          oidPtr(did),
		QueryID:       bigintPtr(qid),
		Calls:         bigintPtr(calls),
		TotalExecTime: doublePtr(execTime),
		Rows:          bigintPtr(rows),
	}
}

// --- compositeKey ---

func TestCompositeKey(t *testing.T) {
	r := PgStatStatementsRow{
		QueryID: bigintPtr(100),
		UserID:  oidPtr(10),
		DbID:    oidPtr(1),
	}
	assert.Equal(t, "100_10_1", compositeKey(&r))
}

func TestCompositeKey_NilFields(t *testing.T) {
	r := PgStatStatementsRow{}
	assert.Equal(t, "0_0_0", compositeKey(&r))
}

// --- toSnapshot ---

func TestToSnapshot(t *testing.T) {
	rows := []PgStatStatementsRow{
		fakeStatementsRow(1, 10, 100, 5, 10.0, 50),
		fakeStatementsRow(2, 10, 100, 3, 6.0, 30),
	}
	snap := toSnapshot(rows)
	assert.Len(t, snap, 2)
	assert.Contains(t, snap, "1_10_100")
	assert.Contains(t, snap, "2_10_100")
}

func TestToSnapshot_SkipsNullIdentifiers(t *testing.T) {
	rows := []PgStatStatementsRow{
		{UserID: oidPtr(1), DbID: oidPtr(1)},       // no QueryID
		{QueryID: bigintPtr(1), DbID: oidPtr(1)},   // no UserID
		{QueryID: bigintPtr(1), UserID: oidPtr(1)}, // no DbID
	}
	snap := toSnapshot(rows)
	assert.Empty(t, snap)
}

// --- bigintDiff ---

func TestBigintDiff(t *testing.T) {
	tests := []struct {
		name string
		prev *Bigint
		curr *Bigint
		want *Bigint
	}{
		{"positive diff", bigintPtr(5), bigintPtr(10), bigintPtr(5)},
		{"zero diff", bigintPtr(10), bigintPtr(10), bigintPtr(0)},
		{"negative diff (reset)", bigintPtr(10), bigintPtr(5), nil},
		{"nil prev", nil, bigintPtr(10), nil},
		{"nil curr", bigintPtr(10), nil, nil},
		{"both nil", nil, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bigintDiff(tt.prev, tt.curr)
			if tt.want == nil {
				assert.Nil(t, got)
			} else {
				require.NotNil(t, got)
				assert.Equal(t, *tt.want, *got)
			}
		})
	}
}

// --- doubleDiff ---

func TestDoubleDiff(t *testing.T) {
	tests := []struct {
		name string
		prev *DoublePrecision
		curr *DoublePrecision
		want *DoublePrecision
	}{
		{"positive diff", doublePtr(5.0), doublePtr(10.0), doublePtr(5.0)},
		{"zero diff", doublePtr(10.0), doublePtr(10.0), doublePtr(0.0)},
		{"negative diff (reset)", doublePtr(10.0), doublePtr(5.0), nil},
		{"nil prev", nil, doublePtr(10.0), nil},
		{"nil curr", doublePtr(10.0), nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := doubleDiff(tt.prev, tt.curr)
			if tt.want == nil {
				assert.Nil(t, got)
			} else {
				require.NotNil(t, got)
				assert.InDelta(t, float64(*tt.want), float64(*got), 1e-9)
			}
		})
	}
}

// --- calculateStatementDeltas ---

func TestCalculateStatementDeltas_BasicDiff(t *testing.T) {
	prev := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 5, 10.0, 50),
	}
	curr := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 20.0, 100),
	}

	deltas, count := calculateStatementDeltas(prev, curr, PgStatStatementsDiffLimit)
	assert.Equal(t, 1, count)
	require.Len(t, deltas, 1)

	d := deltas[0]
	assert.Equal(t, Bigint(5), *d.Calls)
	assert.Equal(t, DoublePrecision(10.0), *d.TotalExecTime)
	assert.Equal(t, Bigint(50), *d.Rows)
	// Identifiers should be from current
	assert.Equal(t, Bigint(1), *d.QueryID)
}

func TestCalculateStatementDeltas_NewQueryInCurrent(t *testing.T) {
	prev := map[string]PgStatStatementsRow{}
	curr := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 5, 10.0, 50),
	}

	deltas, count := calculateStatementDeltas(prev, curr, PgStatStatementsDiffLimit)
	assert.Equal(t, 1, count)
	require.Len(t, deltas, 1)

	// New query: diff is the full cumulative value (prev defaults to 0)
	assert.Equal(t, Bigint(5), *deltas[0].Calls)
}

func TestCalculateStatementDeltas_SkipsNegativeDiff(t *testing.T) {
	prev := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 20.0, 100),
	}
	// Lower cumulative values — indicates a reset
	curr := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 5, 10.0, 50),
	}

	deltas, count := calculateStatementDeltas(prev, curr, PgStatStatementsDiffLimit)
	assert.Equal(t, 0, count)
	assert.Empty(t, deltas)
}

func TestCalculateStatementDeltas_SkipsZeroDiffCalls(t *testing.T) {
	prev := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 20.0, 100),
	}
	// Same calls — no activity
	curr := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 25.0, 110),
	}

	deltas, count := calculateStatementDeltas(prev, curr, PgStatStatementsDiffLimit)
	assert.Equal(t, 0, count)
	assert.Empty(t, deltas)
}

func TestCalculateStatementDeltas_IdentifiersFromCurrent(t *testing.T) {
	prev := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 5, 10.0, 50),
	}

	curr := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 20.0, 100),
	}

	deltas, _ := calculateStatementDeltas(prev, curr, PgStatStatementsDiffLimit)
	require.Len(t, deltas, 1)

	// Identifiers should be from current snapshot
	assert.Equal(t, Oid(10), *deltas[0].UserID)
	assert.Equal(t, Oid(100), *deltas[0].DbID)
	assert.Equal(t, Bigint(1), *deltas[0].QueryID)
}

func TestCalculateStatementDeltas_SortedByAvgExecTimeDesc(t *testing.T) {
	prev := map[string]PgStatStatementsRow{}
	curr := map[string]PgStatStatementsRow{
		// Fast query: 1ms avg (calls=10, exec=10ms)
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 10.0, 10),
		// Slow query: 100ms avg (calls=1, exec=100ms)
		"2_10_100": fakeStatementsRow(2, 10, 100, 1, 100.0, 1),
		// Medium query: 5ms avg (calls=20, exec=100ms)
		"3_10_100": fakeStatementsRow(3, 10, 100, 20, 100.0, 20),
	}

	deltas, _ := calculateStatementDeltas(prev, curr, PgStatStatementsDiffLimit)
	require.Len(t, deltas, 3)

	// Sorted by avg exec time desc: slow (100ms) > medium (5ms) > fast (1ms)
	assert.Equal(t, Bigint(2), *deltas[0].QueryID)
	assert.Equal(t, Bigint(3), *deltas[1].QueryID)
	assert.Equal(t, Bigint(1), *deltas[2].QueryID)
}

func TestCalculateStatementDeltas_CappedAtLimit(t *testing.T) {
	prev := map[string]PgStatStatementsRow{}
	curr := make(map[string]PgStatStatementsRow)

	for i := int64(1); i <= 10; i++ {
		key := compositeKey(&PgStatStatementsRow{
			QueryID: bigintPtr(i), UserID: oidPtr(10), DbID: oidPtr(100),
		})
		curr[key] = fakeStatementsRow(i, 10, 100, i, float64(i)*10, i)
	}

	deltas, count := calculateStatementDeltas(prev, curr, 3)
	assert.Equal(t, 10, count) // total diffs before cap
	assert.Len(t, deltas, 3)   // capped
}

// --- calculateAvgRuntime ---

func TestCalculateAvgRuntime_Basic(t *testing.T) {
	prev := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 5, 100.0, 50),
		"2_10_100": fakeStatementsRow(2, 10, 100, 10, 200.0, 100),
	}
	curr := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 200.0, 100), // +5 calls, +100ms
		"2_10_100": fakeStatementsRow(2, 10, 100, 20, 400.0, 200), // +10 calls, +200ms
	}

	// Total: +15 calls, +300ms exec time → avg = 300/15 = 20ms
	avg := calculateAvgRuntime(prev, curr)
	assert.InDelta(t, 20.0, avg, 1e-9)
}

func TestCalculateAvgRuntime_NewQuery(t *testing.T) {
	prev := map[string]PgStatStatementsRow{}
	curr := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 100.0, 50),
	}

	// New query (not in prev): 10 calls, 100ms → avg = 10ms
	avg := calculateAvgRuntime(prev, curr)
	assert.InDelta(t, 10.0, avg, 1e-9)
}

func TestCalculateAvgRuntime_NoCalls(t *testing.T) {
	prev := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 100.0, 50),
	}
	curr := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 100.0, 50), // no change
	}

	avg := calculateAvgRuntime(prev, curr)
	assert.Equal(t, 0.0, avg)
}

func TestCalculateAvgRuntime_SkipsResets(t *testing.T) {
	prev := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 10, 100.0, 50),
	}
	curr := map[string]PgStatStatementsRow{
		"1_10_100": fakeStatementsRow(1, 10, 100, 5, 50.0, 25), // reset
	}

	avg := calculateAvgRuntime(prev, curr)
	assert.Equal(t, 0.0, avg)
}

func TestCalculateAvgRuntime_EmptyMaps(t *testing.T) {
	avg := calculateAvgRuntime(
		map[string]PgStatStatementsRow{},
		map[string]PgStatStatementsRow{},
	)
	assert.Equal(t, 0.0, avg)
}

// --- buildPgStatStatementsQuery ---

func TestBuildPgStatStatementsQuery_IncludeQueries(t *testing.T) {
	q := buildPgStatStatementsQuery(true, 1000, 16)
	assert.Contains(t, q, "LEFT(query, 1000)")
	assert.Contains(t, q, "LENGTH(query)")
	assert.Contains(t, q, "AS query_len")
}

func TestBuildPgStatStatementsQuery_ExcludeQueries(t *testing.T) {
	q := buildPgStatStatementsQuery(false, 1000, 16)
	assert.Contains(t, q, "NULL::text AS query")
	assert.Contains(t, q, "NULL::bigint AS query_len")
	assert.NotContains(t, q, "LEFT(query")
}

func TestBuildPgStatStatementsQuery_VersionGating(t *testing.T) {
	// PG13: no toplevel, no temp_blk_read_time, no jit_*, aliases blk_read_time → shared_blk_read_time
	q13 := buildPgStatStatementsQuery(true, 1000, 13)
	assert.NotContains(t, q13, "toplevel")
	assert.NotContains(t, q13, "temp_blk_read_time")
	assert.NotContains(t, q13, "jit_functions")
	assert.Contains(t, q13, "blk_read_time AS shared_blk_read_time")
	assert.Contains(t, q13, "blk_write_time AS shared_blk_write_time")
	assert.NotContains(t, q13, "local_blk_read_time")

	// PG14: adds toplevel, still no temp_blk_read_time/jit_*
	q14 := buildPgStatStatementsQuery(true, 1000, 14)
	assert.Contains(t, q14, "toplevel")
	assert.NotContains(t, q14, "temp_blk_read_time")
	assert.NotContains(t, q14, "jit_functions")
	assert.Contains(t, q14, "blk_read_time AS shared_blk_read_time")

	// PG15: adds temp_blk_read_time, jit_*
	q15 := buildPgStatStatementsQuery(true, 1000, 15)
	assert.Contains(t, q15, "toplevel")
	assert.Contains(t, q15, "temp_blk_read_time")
	assert.Contains(t, q15, "jit_functions")
	assert.Contains(t, q15, "blk_read_time AS shared_blk_read_time")
	assert.NotContains(t, q15, "local_blk_read_time")

	// PG17: uses shared_blk_read_time directly, adds local_blk_read_time
	q17 := buildPgStatStatementsQuery(true, 1000, 17)
	assert.Contains(t, q17, "shared_blk_read_time")
	assert.Contains(t, q17, "shared_blk_write_time")
	assert.NotContains(t, q17, "blk_read_time AS") // no alias needed
	assert.Contains(t, q17, "local_blk_read_time")
	assert.Contains(t, q17, "local_blk_write_time")
}

// --- PgStatStatementsPayload JSON ---

func TestPgStatStatementsPayload_JSON(t *testing.T) {
	payload := PgStatStatementsPayload{
		Rows: []PgStatStatementsRow{
			fakeStatementsRow(1, 10, 100, 5, 10.0, 50),
		},
		DeltaCount:          0,
		AverageQueryRuntime: 0.0,
	}

	data, err := json.Marshal(payload)
	require.NoError(t, err)

	var decoded PgStatStatementsPayload
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Len(t, decoded.Rows, 1)
	assert.Nil(t, decoded.Deltas) // omitempty
	assert.Equal(t, 0, decoded.DeltaCount)
}

func TestPgStatStatementsPayload_JSON_WithDeltas(t *testing.T) {
	payload := PgStatStatementsPayload{
		Rows: []PgStatStatementsRow{
			fakeStatementsRow(1, 10, 100, 10, 20.0, 100),
		},
		Deltas: []PgStatStatementsDelta{
			{
				UserID: oidPtr(10), DbID: oidPtr(100), QueryID: bigintPtr(1),
				Calls: bigintPtr(5), TotalExecTime: doublePtr(10.0), Rows: bigintPtr(50),
			},
		},
		DeltaCount:          1,
		AverageQueryRuntime: 2.0,
	}

	data, err := json.Marshal(payload)
	require.NoError(t, err)

	var decoded PgStatStatementsPayload
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Len(t, decoded.Deltas, 1)
	assert.Equal(t, 1, decoded.DeltaCount)
	assert.InDelta(t, 2.0, decoded.AverageQueryRuntime, 1e-9)
}

func TestPgStatStatementsRow_QueryOmittedWhenNil(t *testing.T) {
	row := fakeStatementsRow(1, 10, 100, 5, 10.0, 50)
	// Query is nil by default in fakeStatementsRow

	data, err := json.Marshal(row)
	require.NoError(t, err)

	assert.NotContains(t, string(data), `"query"`)
	assert.NotContains(t, string(data), `"query_len"`)
}

func TestPgStatStatementsRow_QueryIncludedWhenSet(t *testing.T) {
	row := fakeStatementsRow(1, 10, 100, 5, 10.0, 50)
	row.Query = textPtr("SELECT 1")
	row.QueryLen = bigintPtr(8)

	data, err := json.Marshal(row)
	require.NoError(t, err)

	assert.Contains(t, string(data), `"query":"SELECT 1"`)
	assert.Contains(t, string(data), `"query_len":8`)
}

// --- zeroIfNil ---

func TestZeroIfNil(t *testing.T) {
	t.Run("exists=false returns zero", func(t *testing.T) {
		got := zeroIfNil(nil, false)
		require.NotNil(t, got)
		assert.Equal(t, Bigint(0), *got)
	})

	t.Run("exists=true returns value", func(t *testing.T) {
		val := bigintPtr(42)
		got := zeroIfNil(val, true)
		assert.Equal(t, val, got)
	})

	t.Run("exists=true nil value returns nil", func(t *testing.T) {
		got := zeroIfNil(nil, true)
		assert.Nil(t, got)
	})
}
