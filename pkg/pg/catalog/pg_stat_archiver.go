package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatArchiverName     = "pg_stat_archiver"
	PgStatArchiverInterval = 1 * time.Minute
)

const pgStatArchiverQuery = `
SELECT
    archived_count, last_archived_wal,
    last_archived_time::text AS last_archived_time,
    failed_count, last_failed_wal,
    last_failed_time::text AS last_failed_time,
    stats_reset::text AS stats_reset
FROM pg_stat_archiver
`

func CollectPgStatArchiver(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatArchiverRow, error) {
	return CollectView[agent.PgStatArchiverRow](pgPool, ctx, pgStatArchiverQuery, "pg_stat_archiver")
}

func NewPgStatArchiverCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatArchiverName,
		Interval: PgStatArchiverInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatArchiver(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatArchiverPayload{Rows: rows}, nil
		},
	}
}
