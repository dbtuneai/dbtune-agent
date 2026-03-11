package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PG 15+ only.
const pgStatRecoveryPrefetchQuery = `
SELECT
    stats_reset::text AS stats_reset,
    prefetch, hit,
    skip_init, skip_new, skip_fpw, skip_rep,
    wal_distance, block_distance, io_depth
FROM pg_stat_recovery_prefetch
`

func CollectPgStatRecoveryPrefetch(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatRecoveryPrefetchRow, error) {
	if pgMajorVersion < 15 {
		return nil, nil
	}
	return CollectView[agent.PgStatRecoveryPrefetchRow](pgPool, ctx, pgStatRecoveryPrefetchQuery, "pg_stat_recovery_prefetch")
}
