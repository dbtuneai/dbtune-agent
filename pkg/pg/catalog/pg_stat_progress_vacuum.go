package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatProgressVacuumName     = "pg_stat_progress_vacuum"
	PgStatProgressVacuumInterval = 30 * time.Second
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

func NewPgStatProgressVacuumCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatProgressVacuumName,
		Interval: PgStatProgressVacuumInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatProgressVacuum(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatProgressVacuumPayload{Rows: rows}, nil
		},
	}
}
