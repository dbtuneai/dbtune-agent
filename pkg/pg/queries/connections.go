package queries

// Connections queries active, idle, and idle-in-transaction connection counts
// from pg_stat_activity.
//
// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ConnectionStatsRow struct {
	Active                 Bigint `json:"active_connections" db:"active_connections"`
	Idle                   Bigint `json:"idle_connections" db:"idle_connections"`
	IdleInTransaction      Bigint `json:"idle_in_transaction_connections" db:"idle_in_transaction_connections"`
	IdleInTransactionAbort Bigint `json:"idle_in_transaction_aborted_connections" db:"idle_in_transaction_aborted_connections"`
	Total                  Bigint `json:"total_connections" db:"total_connections"`
}

const (
	ConnectionStatsName     = "database_connections"
	ConnectionStatsInterval = 5 * time.Second
)

const connectionStatsQuery = `
SELECT
    COUNT(*) FILTER (WHERE state = 'active') AS active_connections,
    COUNT(*) FILTER (WHERE state = 'idle') AS idle_connections,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction_connections,
    COUNT(*) FILTER (WHERE state = 'idle in transaction (aborted)') AS idle_in_transaction_aborted_connections,
    COUNT(*) FILTER (WHERE state IN ('active', 'idle', 'idle in transaction', 'idle in transaction (aborted)')) AS total_connections
FROM pg_stat_activity`

func ConnectionStatsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[ConnectionStatsRow](pool, prepareCtx, ConnectionStatsName, ConnectionStatsInterval, connectionStatsQuery)
}
