package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SUBSCRIPTION-STATS

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatSubscriptionStatsRow represents a row from pg_stat_subscription_stats (PG 15+).
type PgStatSubscriptionStatsRow struct {
	SubID           *Oid         `json:"subid" db:"subid"`
	SubName         *Name        `json:"subname" db:"subname"`
	ApplyErrorCount *Bigint      `json:"apply_error_count" db:"apply_error_count"`
	SyncErrorCount  *Bigint      `json:"sync_error_count" db:"sync_error_count"`
	StatsReset      *TimestampTZ `json:"stats_reset" db:"stats_reset"`
}

const (
	PgStatSubscriptionStatsName     = "pg_stat_subscription_stats"
	PgStatSubscriptionStatsInterval = 1 * time.Minute
)

// PG 15+ only.
const pgStatSubscriptionStatsQuery = `SELECT * FROM pg_stat_subscription_stats`

func PgStatSubscriptionStatsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	return NewCollector[PgStatSubscriptionStatsRow](pool, prepareCtx, PgStatSubscriptionStatsName, PgStatSubscriptionStatsInterval, pgStatSubscriptionStatsQuery, WithMinPGVersion(pgMajorVersion, 15), WithSkipUnchanged())
}
