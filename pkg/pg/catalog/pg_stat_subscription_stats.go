package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatSubscriptionStatsName     = "pg_stat_subscription_stats"
	PgStatSubscriptionStatsInterval = 1 * time.Minute
)

// PG 15+ only.
const pgStatSubscriptionStatsQuery = `
SELECT
    subid, subname,
    apply_error_count, sync_error_count,
    stats_reset::text AS stats_reset
FROM pg_stat_subscription_stats
`

func CollectPgStatSubscriptionStats(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatSubscriptionStatsRow, error) {
	if pgMajorVersion < 15 {
		return nil, nil
	}
	return CollectView[agent.PgStatSubscriptionStatsRow](pgPool, ctx, pgStatSubscriptionStatsQuery, "pg_stat_subscription_stats")
}

func NewPgStatSubscriptionStatsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatSubscriptionStatsName,
		Interval: PgStatSubscriptionStatsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatSubscriptionStats(pool, ctx, pgMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &agent.PgStatSubscriptionStatsPayload{Rows: rows}, nil
		},
	}
}
