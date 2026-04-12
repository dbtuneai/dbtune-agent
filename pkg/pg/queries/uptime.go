package queries

// UptimeMinutes reports how long the PostgreSQL server has been running,
// in minutes, by computing the difference between the current timestamp and
// pg_postmaster_start_time().
//
// https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-INFO-SESSION

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type UptimeMinutesRow struct {
	UptimeMinutes DoublePrecision `json:"uptime_minutes" db:"uptime_minutes"`
}

const (
	UptimeMinutesName     = "server_uptime"
	UptimeMinutesInterval = 5 * time.Second
)

const uptimeMinutesQuery = `SELECT EXTRACT(EPOCH FROM (current_timestamp - pg_postmaster_start_time())) / 60 AS uptime_minutes`

func UptimeMinutesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[UptimeMinutesRow](pool, prepareCtx, UptimeMinutesName, UptimeMinutesInterval, uptimeMinutesQuery)
}
