package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SUBSCRIPTION-STATS

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatSubscriptionStatsRow represents a row from pg_stat_subscription_stats (PG 15+).
type PgStatSubscriptionStatsRow struct {
	SubID           *int64  `json:"subid" db:"subid"`
	SubName         *string `json:"subname" db:"subname"`
	ApplyErrorCount *int64  `json:"apply_error_count" db:"apply_error_count"`
	SyncErrorCount  *int64  `json:"sync_error_count" db:"sync_error_count"`
	StatsReset      *string `json:"stats_reset" db:"stats_reset"`
}

type PgStatSubscriptionStatsPayload struct {
	Rows []PgStatSubscriptionStatsRow `json:"rows"`
}

const (
	PgStatSubscriptionStatsName     = "pg_stat_subscription_stats"
	PgStatSubscriptionStatsInterval = 1 * time.Minute
)

// PG 15+ only.
const pgStatSubscriptionStatsQuery = `SELECT * FROM pg_stat_subscription_stats`

func CollectPgStatSubscriptionStats(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]PgStatSubscriptionStatsRow, error) {
	if pgMajorVersion < 15 {
		return nil, nil
	}
	return CollectView[PgStatSubscriptionStatsRow](pgPool, ctx, pgStatSubscriptionStatsQuery, "pg_stat_subscription_stats")
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
			return &PgStatSubscriptionStatsPayload{Rows: rows}, nil
		},
	}
}
