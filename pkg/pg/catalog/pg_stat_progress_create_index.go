package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatProgressCreateIndexName     = "pg_stat_progress_create_index"
	PgStatProgressCreateIndexInterval = 30 * time.Second
)

const pgStatProgressCreateIndexQuery = `
SELECT
    pid, datid, datname, relid, index_relid,
    command, phase,
    lockers_total, lockers_done, current_locker_pid,
    blocks_total, blocks_done,
    tuples_total, tuples_done,
    partitions_total, partitions_done
FROM pg_stat_progress_create_index
`

func CollectPgStatProgressCreateIndex(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatProgressCreateIndexRow, error) {
	return CollectView[agent.PgStatProgressCreateIndexRow](pgPool, ctx, pgStatProgressCreateIndexQuery, "pg_stat_progress_create_index")
}

func NewPgStatProgressCreateIndexCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatProgressCreateIndexName,
		Interval: PgStatProgressCreateIndexInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatProgressCreateIndex(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatProgressCreateIndexPayload{Rows: rows}, nil
		},
	}
}
