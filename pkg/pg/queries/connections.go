package queries

// Connections queries active, idle, and idle-in-transaction connection counts
// from pg_stat_activity.
//
// These metrics help monitor connection pool utilization and detect connection
// leaks (e.g. growing idle-in-transaction counts).
//
// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ConnectionStatsName     = "database_connections"
	ConnectionStatsInterval = 5 * time.Second
)

const ConnectionStatsQuery = `
SELECT
    COUNT(*) FILTER (WHERE state = 'active') AS active_connections,
    COUNT(*) FILTER (WHERE state = 'idle') AS idle_connections,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction_connections,
    COUNT(*) FILTER (WHERE state = 'idle in transaction (aborted)') AS idle_in_transaction_aborted_connections
FROM pg_stat_activity`

// ConnectionStatsPayload holds connection counts by state.
type ConnectionStatsPayload struct {
	Active                              int `json:"active_connections"`
	Idle                                int `json:"idle_connections"`
	IdleInTransaction                   int `json:"idle_in_transaction_connections"`
	IdleInTransactionAbortedConnections int `json:"idle_in_transaction_aborted_connections"`
	Total                               int `json:"total_connections"`
}

// CollectConnectionStats returns active, idle, and idle-in-transaction connection counts.
func CollectConnectionStats(pool *pgxpool.Pool, ctx context.Context) (ConnectionStatsPayload, error) {
	var stats ConnectionStatsPayload
	err := utils.QueryRowWithPrefix(pool, ctx, ConnectionStatsQuery).Scan(
		&stats.Active, &stats.Idle, &stats.IdleInTransaction, &stats.IdleInTransactionAbortedConnections,
	)
	if err != nil {
		return stats, err
	}
	stats.Total = stats.Active + stats.Idle + stats.IdleInTransaction + stats.IdleInTransactionAbortedConnections
	return stats, nil
}

func ConnectionStatsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
		Name:     ConnectionStatsName,
		Interval: ConnectionStatsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			stats, err := CollectConnectionStats(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &stats, nil
		},
	}
}
