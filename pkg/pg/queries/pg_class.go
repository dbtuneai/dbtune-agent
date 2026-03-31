package queries

// https://www.postgresql.org/docs/current/catalog-pg-class.html

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgClassName     = "pg_class"
	PgClassInterval = 1 * time.Minute

	// PgClassBackfillBatchSize controls how many tables are fetched per tick
	// during the initial backfill. After all batches are sent, the collector
	// switches to delta mode using VACUUM/ANALYZE timestamps.
	PgClassBackfillBatchSize = 500
)

// pgClassColumns is the SELECT list shared by batch and delta queries.
const pgClassColumns = `
    n.nspname AS schemaname,
    c.relname,
    c.reltuples,
    c.relpages,
    c.relfrozenxid,
    c.relminmxid`

// pgClassQueryBatch paginates through user tables during backfill.
const pgClassQueryBatch = `SELECT` + pgClassColumns + `
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN (
    SELECT schemaname, relname
    FROM pg_stat_user_tables
    ORDER BY schemaname, relname
    LIMIT $1 OFFSET $2
) batch ON n.nspname = batch.schemaname
       AND c.relname = batch.relname
WHERE c.relkind = 'r'
ORDER BY n.nspname, c.relname
`

// pgClassQueryDelta returns rows only for tables that have been vacuumed or
// analyzed since $1.
const pgClassQueryDelta = `SELECT` + pgClassColumns + `
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_stat_user_tables pst
  ON n.nspname = pst.schemaname
 AND c.relname = pst.relname
WHERE c.relkind = 'r'
  AND GREATEST(pst.last_analyze, pst.last_autoanalyze,
               pst.last_vacuum, pst.last_autovacuum) > $1
ORDER BY n.nspname, c.relname
`

// PgClassRow represents a single row from pg_class for user tables.
type PgClassRow struct {
	SchemaName   Name    `json:"schemaname"`
	RelName      Name    `json:"relname"`
	RelTuples    Real    `json:"reltuples"`
	RelPages     Integer `json:"relpages"`
	RelFrozenXID Xid     `json:"relfrozenxid"`
	RelMinXID    Xid     `json:"relminxid"`
}

func PgClassCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, backfillBatchSize int) CatalogCollector {
	var (
		backfillOffset = 0
		backfillDone   = false
		lastPoll       time.Time
	)

	scanner := pgxutil.NewScanner[PgClassRow]()

	return CatalogCollector{
		Name:     PgClassName,
		Interval: PgClassInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}

			var classRows []PgClassRow

			if !backfillDone {
				classRows, err = CollectView(ctx, pool, pgClassQueryBatch, PgClassName, scanner, WithQueryArgs(backfillBatchSize, backfillOffset))
				if err != nil {
					return nil, err
				}
				if len(classRows) == 0 {
					backfillDone = true
					lastPoll = time.Now().UTC()
				} else {
					backfillOffset += backfillBatchSize
				}
			} else {
				now := time.Now().UTC()
				classRows, err = CollectView(ctx, pool, pgClassQueryDelta, PgClassName, scanner, WithQueryArgs(lastPoll))
				if err != nil {
					return nil, err
				}
				lastPoll = now
			}

			if len(classRows) == 0 {
				return nil, nil
			}

			data, err := json.Marshal(&Payload[PgClassRow]{Rows: classRows})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", PgClassName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
