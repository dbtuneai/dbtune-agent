package catalog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fakeClassRow(schema, table string) PgClassRow {
	return PgClassRow{SchemaName: schema, RelName: table, RelTuples: 100, RelPages: 10}
}

func TestPgClassCollector(t *testing.T) {
	type call struct {
		wantMode      string // "backfill" or "delta"
		wantBatchSize int
		wantOffset    int
		returnRows    []PgClassRow
		returnErr     error
		wantPayload   bool
		wantNilData   bool
		wantErr       bool
	}

	tests := []struct {
		name      string
		batchSize int
		calls     []call
	}{
		{
			name:      "single batch then delta",
			batchSize: 500,
			calls: []call{
				{
					wantMode:      "backfill",
					wantBatchSize: 500,
					wantOffset:    0,
					returnRows:    []PgClassRow{fakeClassRow("public", "users")},
					wantPayload:   true,
				},
				{
					wantMode:      "backfill",
					wantBatchSize: 500,
					wantOffset:    500,
					returnRows:    nil,
					wantNilData:   true,
				},
				{
					wantMode:    "delta",
					returnRows:  []PgClassRow{fakeClassRow("public", "users")},
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
					returnRows:    []PgClassRow{fakeClassRow("public", "t1"), fakeClassRow("public", "t2")},
					wantPayload:   true,
				},
				{
					wantMode:      "backfill",
					wantBatchSize: 2,
					wantOffset:    2,
					returnRows:    []PgClassRow{fakeClassRow("public", "t3")},
					wantPayload:   true,
				},
				{
					wantMode:      "backfill",
					wantBatchSize: 2,
					wantOffset:    4,
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
					wantMode:      "backfill",
					wantBatchSize: 500,
					wantOffset:    0,
					returnRows:    []PgClassRow{fakeClassRow("public", "t1")},
					wantPayload:   true,
				},
			},
		},
		{
			name:      "delta error does not prevent next delta",
			batchSize: 500,
			calls: []call{
				{
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

			collectFn := func(ctx context.Context, q PgClassQueryMode) ([]PgClassRow, error) {
				require.Less(t, callIdx, len(tt.calls), "unexpected extra call")
				c := tt.calls[callIdx]

				if c.wantMode == "backfill" {
					assert.Nil(t, q.since, "call %d: expected backfill mode", callIdx)
					assert.Equal(t, c.wantBatchSize, q.batchSize, "call %d: batchSize", callIdx)
					assert.Equal(t, c.wantOffset, q.offset, "call %d: offset", callIdx)
				} else {
					assert.NotNil(t, q.since, "call %d: expected delta mode", callIdx)
				}

				if c.returnErr != nil {
					return nil, c.returnErr
				}
				return c.returnRows, nil
			}

			collect := newPgClassCollectFunc(collectFn, tt.batchSize)

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
					payload, ok := result.(*PgClassPayload)
					require.True(t, ok, "call %d: expected *PgClassPayload", i)
					assert.Equal(t, c.returnRows, payload.Rows, "call %d: rows mismatch", i)
				}
			}
		})
	}
}

func TestPgClassQueryModeConstructors(t *testing.T) {
	t.Run("backfill query", func(t *testing.T) {
		q := PgClassBackfillQuery(100, 200)
		assert.Equal(t, 100, q.batchSize)
		assert.Equal(t, 200, q.offset)
		assert.Nil(t, q.since)
	})

	t.Run("delta query", func(t *testing.T) {
		ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		q := PgClassDeltaQuery(ts)
		assert.Equal(t, 0, q.batchSize)
		assert.Equal(t, 0, q.offset)
		require.NotNil(t, q.since)
		assert.Equal(t, ts, *q.since)
	})
}
