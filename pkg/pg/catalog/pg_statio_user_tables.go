package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgStatioUserTablesQuery = `
SELECT relid::bigint AS relid, schemaname, relname,
    heap_blks_read, heap_blks_hit,
    idx_blks_read, idx_blks_hit,
    toast_blks_read, toast_blks_hit,
    tidx_blks_read, tidx_blks_hit
FROM pg_statio_user_tables
WHERE COALESCE(heap_blks_read,0) + COALESCE(heap_blks_hit,0) +
      COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0) > 0
ORDER BY COALESCE(heap_blks_read,0) + COALESCE(idx_blks_read,0) DESC
LIMIT 500
`

func CollectPgStatioUserTables(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatioUserTablesRow, error) {
	return CollectView[agent.PgStatioUserTablesRow](pgPool, ctx, pgStatioUserTablesQuery, "pg_statio_user_tables")
}
