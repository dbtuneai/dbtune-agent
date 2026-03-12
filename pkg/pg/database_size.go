package pg

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DatabaseSizeQuery = `SELECT sum(pg_database_size(datname)) as total_size_bytes FROM pg_database`

// DatabaseSize returns the size of all the databases combined in bytes
func DatabaseSize(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var totalSizeBytes int64
		err := utils.QueryRowWithPrefix(pgPool, ctx, DatabaseSizeQuery).Scan(&totalSizeBytes)
		if err != nil {
			return err
		}

		metricEntry, err := metrics.PGInstanceSize.AsFlatValue(totalSizeBytes)
		if err != nil {
			return err
		}
		state.AddMetric(metricEntry)

		return nil
	}
}
