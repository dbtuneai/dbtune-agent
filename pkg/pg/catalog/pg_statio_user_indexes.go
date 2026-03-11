package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
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
