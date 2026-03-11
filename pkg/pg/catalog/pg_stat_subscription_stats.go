package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
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
