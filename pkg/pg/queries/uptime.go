package queries

// UptimeMinutes reports how long the PostgreSQL server has been running,
// in minutes, by computing the difference between the current timestamp and
// pg_postmaster_start_time().
//
// https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-INFO-SESSION

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	UptimeMinutesName     = "server_uptime"
	UptimeMinutesInterval = 5 * time.Second
)

const UptimeMinutesQuery = `SELECT EXTRACT(EPOCH FROM (current_timestamp - pg_postmaster_start_time())) / 60 as uptime_minutes`

// CollectUptimeMinutes returns how long the PostgreSQL server has been running, in minutes.
func CollectUptimeMinutes(pool *pgxpool.Pool, ctx context.Context) (float64, error) {
	var uptime float64
	err := utils.QueryRowWithPrefix(pool, ctx, UptimeMinutesQuery).Scan(&uptime)
	return uptime, err
}

type UptimeMinutesPayload struct {
	UptimeMinutes float64 `json:"uptime_minutes"`
}

func UptimeMinutesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
		Name:     UptimeMinutesName,
		Interval: UptimeMinutesInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			uptime, err := CollectUptimeMinutes(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &UptimeMinutesPayload{UptimeMinutes: uptime}, nil
		},
	}
}
