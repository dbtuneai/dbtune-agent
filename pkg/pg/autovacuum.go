package pg

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

// https://stackoverflow.com/a/25012622
const AutovacuumQuery = `SELECT COUNT(*) FROM pg_stat_activity WHERE starts_with(query, 'autovacuum:')`

func Autovacuum(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var result int
		err := utils.QueryRowWithPrefix(pgPool, ctx, AutovacuumQuery).Scan(&result)
		if err != nil {
			return err
		}

		metricEntry, err := metrics.PGAutoVacuumCount.AsFlatValue(result)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		return nil
	}
}
