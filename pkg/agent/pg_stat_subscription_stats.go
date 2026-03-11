package agent

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
