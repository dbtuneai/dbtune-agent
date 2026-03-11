package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatRecoveryPrefetchName     = "pg_stat_recovery_prefetch"
	PgStatRecoveryPrefetchInterval = 1 * time.Minute
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

func NewPgStatRecoveryPrefetchCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatRecoveryPrefetchName,
		Interval: PgStatRecoveryPrefetchInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatRecoveryPrefetch(pool, ctx, pgMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &agent.PgStatRecoveryPrefetchPayload{Rows: rows}, nil
		},
	}
}
