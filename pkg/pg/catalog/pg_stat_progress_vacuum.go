package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgStatProgressVacuumQuery = `
SELECT
    pid, datid, datname, relid,
    phase,
    heap_blks_total, heap_blks_scanned, heap_blks_vacuumed,
    index_vacuum_count, max_dead_tuples, num_dead_tuples
FROM pg_stat_progress_vacuum
`

func CollectPgStatProgressVacuum(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatProgressVacuumRow, error) {
	return CollectView[agent.PgStatProgressVacuumRow](pgPool, ctx, pgStatProgressVacuumQuery, "pg_stat_progress_vacuum")
}
