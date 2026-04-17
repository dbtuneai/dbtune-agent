package queries

// https://www.postgresql.org/docs/current/catalog-pg-class.html

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgClassName     = "pg_class"
	PgClassInterval = 1 * time.Minute

	// PgClassBackfillBatchSize controls how many tables are fetched per tick
	// during the initial backfill. After all batches are sent, the collector
	// switches to delta mode using VACUUM/ANALYZE timestamps.
	PgClassBackfillBatchSize = 500

	// pgClassFullRescanInterval controls how many delta ticks pass before a
	// full rescan is triggered. Delta mode only covers regular tables (via
	// pg_stat_user_tables); indexes and materialized views are only captured
	// during backfill or full rescans. At the default 1-minute interval,
	// 30 ticks = 30 minutes between full rescans.
	pgClassFullRescanInterval = 30
)

// pgClassColumns is the SELECT list shared by batch and delta queries.
const pgClassColumns = `
	c.oid AS oid,
    n.nspname AS schemaname,
    c.relname,
    c.relkind::text AS relkind,
    c.reltuples,
    c.relpages,
    c.relfrozenxid,
    c.relminmxid,
    c.reloptions::text[] AS reloptions,
    am.amname AS access_method,
    c.relallvisible,
    c.relpersistence::text AS relpersistence,
    c.relispartition,
    c.relhassubclass,
    c.reltoastrelid,
    c.reltablespace`

// pgClassQueryBatch paginates through user tables, indexes, and materialized
// views during backfill. Indexes and matviews are included so consumers get
// reloptions (fillfactor) and access_method without relying on DDL parsing.
const pgClassQueryBatch = `SELECT` + pgClassColumns + `
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r', 'i', 'm')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY n.nspname, c.relname
LIMIT $1 OFFSET $2
`

// pgClassQueryDelta returns only regular tables that have been vacuumed or
// analyzed since $1. Indexes and matviews are not covered here; they are
// picked up by periodic full rescans (see pgClassFullRescanInterval).
const pgClassQueryDelta = `SELECT` + pgClassColumns + `
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_am am ON am.oid = c.relam
JOIN pg_stat_user_tables pst
  ON n.nspname = pst.schemaname
 AND c.relname = pst.relname
WHERE c.relkind = 'r'
  AND GREATEST(pst.last_analyze, pst.last_autoanalyze,
               pst.last_vacuum, pst.last_autovacuum) > $1
ORDER BY n.nspname, c.relname
`

// pgClassQueryFullRescan returns all user tables, indexes, and materialized
// views in one shot. Used periodically to pick up structural changes (e.g.
// ALTER INDEX SET fillfactor) that the timestamp-based delta query misses.
const pgClassQueryFullRescan = `SELECT` + pgClassColumns + `
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r', 'i', 'm')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY n.nspname, c.relname
`

// PgClassRow represents a single row from pg_class.
type PgClassRow struct {
	Oid            Oid     `json:"oid"`
	SchemaName     Name    `json:"schemaname"`
	RelName        Name    `json:"relname"`
	RelKind        Text    `json:"relkind"`
	RelTuples      Real    `json:"reltuples"`
	RelPages       Integer `json:"relpages"`
	RelFrozenXID   Xid     `json:"relfrozenxid"`
	RelMinMXID     Xid     `json:"relminmxid"`
	RelOptions     []Text  `json:"reloptions"`
	AccessMethod   *Text   `json:"access_method" db:"access_method"`
	RelAllVisible  Integer `json:"relallvisible"`
	RelPersistence Text    `json:"relpersistence"`
	RelIsPartition Boolean `json:"relispartition"`
	RelHasSubClass Boolean `json:"relhassubclass"`
	RelToastRelID  Oid     `json:"reltoastrelid"`
	RelTablespace  Oid     `json:"reltablespace"`
}

// PgClassConfig holds configuration for the pg_class collector.
type PgClassConfig struct {
	BackfillBatchSize int `config:"backfill_batch_size" default:"500" min:"0"`
}

func PgClassCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, cfg PgClassConfig) CatalogCollector {
	backfillBatchSize := cfg.BackfillBatchSize
	var (
		backfillOffset = 0
		backfillDone   = false
		lastPoll       time.Time
		deltaTicks     int
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

			collectedAt := time.Now().UTC()
			var classRows []PgClassRow

			if !backfillDone {
				querier := func() (pgx.Rows, error) {
					return utils.QueryWithPrefix(pool, ctx, pgClassQueryBatch, backfillBatchSize, backfillOffset)
				}
				classRows, err = CollectView(querier, PgClassName, scanner)
				if err != nil {
					return nil, err
				}
				if len(classRows) == 0 {
					backfillDone = true
					lastPoll = collectedAt
				} else {
					backfillOffset += backfillBatchSize
				}
			} else {
				deltaTicks++
				if deltaTicks%pgClassFullRescanInterval == 0 {
					// Full rescan: re-fetch all relkinds to pick up
					// structural changes missed by delta mode.
					querier := func() (pgx.Rows, error) {
						return utils.QueryWithPrefix(pool, ctx, pgClassQueryFullRescan)
					}
					classRows, err = CollectView(querier, PgClassName, scanner)
				} else {
					querier := func() (pgx.Rows, error) {
						return utils.QueryWithPrefix(pool, ctx, pgClassQueryDelta, lastPoll)
					}
					classRows, err = CollectView(querier, PgClassName, scanner)
				}
				if err != nil {
					return nil, err
				}
				lastPoll = collectedAt
			}

			if len(classRows) == 0 {
				return nil, nil
			}

			data, err := json.Marshal(&Payload[PgClassRow]{CollectedAt: collectedAt, Rows: classRows})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", PgClassName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
