package pg

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const ConnectionStatsQuery = `
SELECT
    COUNT(*) FILTER (WHERE state = 'active') AS active_connections,
    COUNT(*) FILTER (WHERE state = 'idle') AS idle_connections,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction_connections
FROM pg_stat_activity`

func Connections(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var active, idle, idleInTransaction int
		err := utils.QueryRowWithPrefix(pgPool, ctx, ConnectionStatsQuery).Scan(&active, &idle, &idleInTransaction)
		if err != nil {
			return err
		}

		activeEntry, err := metrics.PGActiveConnections.AsFlatValue(active)
		if err != nil {
			return err
		}
		state.AddMetric(activeEntry)

		idleEntry, err := metrics.PGIdleConnections.AsFlatValue(idle)
		if err != nil {
			return err
		}
		state.AddMetric(idleEntry)

		idleInTransactionEntry, err := metrics.PGIdleInTransactionConnections.AsFlatValue(idleInTransaction)
		if err != nil {
			return err
		}
		state.AddMetric(idleInTransactionEntry)

		return nil
	}
}
