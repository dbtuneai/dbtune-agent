package pg

import (
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultMetricCollectors returns the standard set of PostgreSQL metric
// collectors shared across all adapters. Adapter-specific collectors
// (e.g. hardware) should be appended by the caller.
func DefaultMetricCollectors(pool *pgxpool.Pool, pgConfig Config) []agent.MetricCollector {
	return []agent.MetricCollector{
		{
			Key:       "database_average_query_runtime",
			Collector: PGStatStatements(pool, pgConfig.IncludeQueries, pgConfig.MaximumQueryTextLength),
		},
		{
			Key:       "database_transactions_per_second",
			Collector: TransactionsPerSecond(pool),
		},
		{
			Key:       "database_connections",
			Collector: Connections(pool),
		},
		{
			Key:       "system_db_size",
			Collector: DatabaseSize(pool),
		},
		{
			Key:       "database_autovacuum_count",
			Collector: Autovacuum(pool),
		},
		{
			Key:       "server_uptime",
			Collector: UptimeMinutes(pool),
		},
	}
}
