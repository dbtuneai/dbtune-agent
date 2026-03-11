package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgStatSlruQuery = `
SELECT
    name, blks_zeroed, blks_hit, blks_read, blks_written, blks_exists,
    flushes, truncates,
    stats_reset::text AS stats_reset
FROM pg_stat_slru
`

func CollectPgStatSlru(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatSlruRow, error) {
	return CollectView[agent.PgStatSlruRow](pgPool, ctx, pgStatSlruQuery, "pg_stat_slru")
}
