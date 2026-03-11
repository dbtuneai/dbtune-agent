package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STATIO-ALL-TABLES

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatioUserTablesName     = "pg_statio_user_tables"
	PgStatioUserTablesInterval = 1 * time.Minute
)

const pgStatioUserTablesQuery = `
SELECT * FROM pg_statio_user_tables
WHERE COALESCE(heap_blks_read,0) + COALESCE(heap_blks_hit,0) +
      COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0) > 0
ORDER BY COALESCE(heap_blks_read,0) + COALESCE(idx_blks_read,0) DESC
LIMIT 500
`

func CollectPgStatioUserTables(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatioUserTablesRow, error) {
	return CollectView[PgStatioUserTablesRow](pgPool, ctx, pgStatioUserTablesQuery, "pg_statio_user_tables")
}

func NewPgStatioUserTablesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatioUserTablesName,
		Interval: PgStatioUserTablesInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatioUserTables(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatioUserTablesPayload{Rows: rows}, nil
		},
	}
}

// PgStatioUserTablesRow represents a row from pg_statio_user_tables.
type PgStatioUserTablesRow struct {
	RelID         *int64  `json:"relid" db:"relid"`
	SchemaName    *string `json:"schemaname" db:"schemaname"`
	RelName       *string `json:"relname" db:"relname"`
	HeapBlksRead  *int64  `json:"heap_blks_read" db:"heap_blks_read"`
	HeapBlksHit   *int64  `json:"heap_blks_hit" db:"heap_blks_hit"`
	IdxBlksRead   *int64  `json:"idx_blks_read" db:"idx_blks_read"`
	IdxBlksHit    *int64  `json:"idx_blks_hit" db:"idx_blks_hit"`
	ToastBlksRead *int64  `json:"toast_blks_read" db:"toast_blks_read"`
	ToastBlksHit  *int64  `json:"toast_blks_hit" db:"toast_blks_hit"`
	TidxBlksRead  *int64  `json:"tidx_blks_read" db:"tidx_blks_read"`
	TidxBlksHit   *int64  `json:"tidx_blks_hit" db:"tidx_blks_hit"`
}

type PgStatioUserTablesPayload struct {
	Rows []PgStatioUserTablesRow `json:"rows"`
}
