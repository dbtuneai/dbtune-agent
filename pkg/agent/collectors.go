package agent

import (
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultMetricCollectors returns the standard set of PostgreSQL metric
// collectors shared across all adapters. Adapter-specific collectors
// (e.g. hardware) should be appended by the caller.
func DefaultMetricCollectors(pool *pgxpool.Pool, pgConfig pg.Config) []MetricCollector {
	return []MetricCollector{}
}
