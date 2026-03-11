package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatioUserIndexesName     = "pg_statio_user_indexes"
	PgStatioUserIndexesInterval = 1 * time.Minute
)

const pgStatioUserIndexesQuery = `
SELECT relid::bigint AS relid, indexrelid::bigint AS indexrelid,
    schemaname, relname, indexrelname,
    idx_blks_read, idx_blks_hit
FROM pg_statio_user_indexes
WHERE COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0) > 0
`

func CollectPgStatioUserIndexes(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatioUserIndexesRow, error) {
	return CollectView[agent.PgStatioUserIndexesRow](pgPool, ctx, pgStatioUserIndexesQuery, "pg_statio_user_indexes")
}

func NewPgStatioUserIndexesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatioUserIndexesName,
		Interval: PgStatioUserIndexesInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatioUserIndexes(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatioUserIndexesPayload{Rows: rows}, nil
		},
	}
}
