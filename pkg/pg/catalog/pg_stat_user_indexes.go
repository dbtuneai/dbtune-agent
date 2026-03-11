package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatUserIndexesName     = "pg_stat_user_indexes"
	PgStatUserIndexesInterval = 1 * time.Minute
)

const pgStatUserIndexesQuery = `
SELECT
    relid::bigint AS relid,
    indexrelid::bigint AS indexrelid,
    schemaname, relname, indexrelname,
    idx_scan,
    (to_jsonb(i) ->> 'last_idx_scan')::text AS last_idx_scan,
    idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes i
`

func CollectPgStatUserIndexes(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatUserIndexesRow, error) {
	return CollectView[agent.PgStatUserIndexesRow](pgPool, ctx, pgStatUserIndexesQuery, "pg_stat_user_indexes")
}

func NewPgStatUserIndexesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatUserIndexesName,
		Interval: PgStatUserIndexesInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatUserIndexes(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatUserIndexesPayload{Rows: rows}, nil
		},
	}
}
