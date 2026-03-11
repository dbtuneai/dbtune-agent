package catalog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRow creates a PgStatsRow with the given schema/table/column names.
func fakeRow(schema, table, col string) PgStatsRow {
	return PgStatsRow{SchemaName: schema, TableName: table, AttName: col}
}

func TestPgStatsCollector(t *testing.T) {
	type call struct {
		wantMode string // "backfill" or "delta"
		// For backfill mode
		wantBatchSize int
		wantOffset    int
		// Return values from the mock collectFn
		returnRows []PgStatsRow
		returnErr  error
		// Expected results
		wantPayload bool // true if non-nil payload expected
		wantNilData bool // true if nil return expected (no data)
		wantErr     bool
	}

	tests := []struct {
		name      string
		batchSize int
		calls     []call
	}{
		{
			name:      "single batch covers all tables then switches to delta",
			batchSize: 500,
			calls: []call{
				{
					wantMode:      "backfill",
					wantBatchSize: 500,
					wantOffset:    0,
					returnRows:    []PgStatsRow{fakeRow("public", "users", "id"), fakeRow("public", "users", "name")},
					wantPayload:   true,
				},
				{
					wantMode:      "backfill",
					wantBatchSize: 500,
					wantOffset:    500,
					returnRows:    nil, // empty — backfill done
					wantNilData:   true,
				},
				{
					wantMode:    "delta",
					returnRows:  []PgStatsRow{fakeRow("public", "users", "id")},
					wantPayload: true,
				},
			},
		},
		{
			name:      "multiple backfill batches",
			batchSize: 2,
			calls: []call{
				{
					wantMode:      "backfill",
					wantBatchSize: 2,
					wantOffset:    0,
					returnRows:    []PgStatsRow{fakeRow("public", "t1", "a"), fakeRow("public", "t1", "b"), fakeRow("public", "t2", "a")},
					wantPayload:   true,
				},
				{
					wantMode:      "backfill",
					wantBatchSize: 2,
					wantOffset:    2,
					returnRows:    []PgStatsRow{fakeRow("public", "t3", "a")},
					wantPayload:   true,
				},
				{
					wantMode:      "backfill",
					wantBatchSize: 2,
					wantOffset:    4,
					returnRows:    nil, // done
					wantNilData:   true,
				},
				{
					wantMode:    "delta",
					returnRows:  nil, // no tables analyzed
					wantNilData: true,
				},
				{
					wantMode:    "delta",
					returnRows:  []PgStatsRow{fakeRow("public", "t1", "a")},
					wantPayload: true,
				},
			},
		},
		{
			name:      "empty database goes straight to delta",
			batchSize: 500,
			calls: []call{
				{
					wantMode:      "backfill",
					wantBatchSize: 500,
					wantOffset:    0,
					returnRows:    nil,
					wantNilData:   true,
				},
				{
					wantMode:    "delta",
					returnRows:  nil,
					wantNilData: true,
				},
			},
		},
		{
			name:      "empty batch does not advance offset",
			batchSize: 2,
			calls: []call{
				{
					wantMode:      "backfill",
					wantBatchSize: 2,
					wantOffset:    0,
					returnRows:    []PgStatsRow{fakeRow("public", "t1", "a")},
					wantPayload:   true,
				},
				{
					// Partial batch (1 < batchSize=2), offset still advances by batchSize.
					wantMode:      "backfill",
					wantBatchSize: 2,
					wantOffset:    2,
					returnRows:    nil, // empty — ends backfill
					wantNilData:   true,
				},
				{
					// Must be in delta mode now, not backfill at offset 4.
					wantMode:    "delta",
					returnRows:  nil,
					wantNilData: true,
				},
			},
		},
		{
			name:      "backfill error does not advance state",
			batchSize: 500,
			calls: []call{
				{
					wantMode:      "backfill",
					wantBatchSize: 500,
					wantOffset:    0,
					returnErr:     errors.New("connection refused"),
					wantErr:       true,
				},
				{
					// Retry — same offset since error didn't advance state
					wantMode:      "backfill",
					wantBatchSize: 500,
					wantOffset:    0,
					returnRows:    []PgStatsRow{fakeRow("public", "t1", "a")},
					wantPayload:   true,
				},
			},
		},
		{
			name:      "delta error does not prevent next delta",
			batchSize: 500,
			calls: []call{
				{
					// Backfill — empty DB
					wantMode:    "backfill",
					wantBatchSize: 500,
					wantOffset:  0,
					returnRows:  nil,
					wantNilData: true,
				},
				{
					wantMode:  "delta",
					returnErr: errors.New("timeout"),
					wantErr:   true,
				},
				{
					// Should still be in delta mode
					wantMode:    "delta",
					returnRows:  nil,
					wantNilData: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callIdx := 0

			collectFn := func(ctx context.Context, q PgStatsQueryMode) ([]PgStatsRow, error) {
				require.Less(t, callIdx, len(tt.calls), "unexpected extra call to collectFn")
				c := tt.calls[callIdx]

				if c.wantMode == "backfill" {
					assert.Nil(t, q.since, "call %d: expected backfill mode (since=nil)", callIdx)
					assert.Equal(t, c.wantBatchSize, q.batchSize, "call %d: batchSize", callIdx)
					assert.Equal(t, c.wantOffset, q.offset, "call %d: offset", callIdx)
				} else {
					assert.NotNil(t, q.since, "call %d: expected delta mode (since!=nil)", callIdx)
				}

				if c.returnErr != nil {
					return nil, c.returnErr
				}
				return c.returnRows, nil
			}

			collect := newPgStatsCollectFunc(collectFn, tt.batchSize)

			for i, c := range tt.calls {
				callIdx = i
				result, err := collect(context.Background())

				if c.wantErr {
					assert.Error(t, err, "call %d: expected error", i)
					continue
				}
				require.NoError(t, err, "call %d", i)

				if c.wantNilData {
					assert.Nil(t, result, "call %d: expected nil result", i)
				}
				if c.wantPayload {
					require.NotNil(t, result, "call %d: expected non-nil payload", i)
					payload, ok := result.(*PgStatsPayload)
					require.True(t, ok, "call %d: expected *PgStatsPayload", i)
					assert.Equal(t, c.returnRows, payload.Rows, "call %d: rows mismatch", i)
				}
			}
		})
	}
}

func TestPgArrayToJSON(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		result, err := pgArrayToJSON(nil)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("valid pg array", func(t *testing.T) {
		s := "{foo,bar,baz}"
		result, err := pgArrayToJSON(&s)
		require.NoError(t, err)
		assert.JSONEq(t, `["foo","bar","baz"]`, string(result))
	})

	t.Run("valid pg array with quoted elements", func(t *testing.T) {
		s := `{foo,"bar baz",qux}`
		result, err := pgArrayToJSON(&s)
		require.NoError(t, err)
		assert.JSONEq(t, `["foo","bar baz","qux"]`, string(result))
	})

	t.Run("valid json passthrough", func(t *testing.T) {
		s := `["already","json"]`
		result, err := pgArrayToJSON(&s)
		require.NoError(t, err)
		assert.JSONEq(t, `["already","json"]`, string(result))
	})

	t.Run("unparseable input returns error", func(t *testing.T) {
		s := "not a valid array or json"
		result, err := pgArrayToJSON(&s)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestPgStatsQueryModeConstructors(t *testing.T) {
	t.Run("backfill query", func(t *testing.T) {
		q := PgStatsBackfillQuery(100, 200)
		assert.Equal(t, 100, q.batchSize)
		assert.Equal(t, 200, q.offset)
		assert.Nil(t, q.since)
	})

	t.Run("delta query", func(t *testing.T) {
		ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		q := PgStatsDeltaQuery(ts)
		assert.Equal(t, 0, q.batchSize)
		assert.Equal(t, 0, q.offset)
		require.NotNil(t, q.since)
		assert.Equal(t, ts, *q.since)
	})
}
