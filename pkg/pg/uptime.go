package pg

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const UptimeMinutesQuery = `
SELECT EXTRACT(EPOCH FROM (current_timestamp - pg_postmaster_start_time())) / 60 as uptime_minutes;
`

func UptimeMinutes(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var uptime float64
		err := utils.QueryRowWithPrefix(pgPool, ctx, UptimeMinutesQuery).Scan(&uptime)
		if err != nil {
			return err
		}

		metricEntry, err := metrics.ServerUptimeMinutes.AsFlatValue(uptime)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		return nil
	}
}
