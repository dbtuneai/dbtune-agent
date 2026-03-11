package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatSlruName     = "pg_stat_slru"
	PgStatSlruInterval = 1 * time.Minute
)

const pgStatSlruQuery = `
SELECT
    name, blks_zeroed, blks_hit, blks_read, blks_written, blks_exists,
    flushes, truncates,
    stats_reset::text AS stats_reset
FROM pg_stat_slru
`

func CollectPgStatSlru(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatSlruRow, error) {
	return CollectView[agent.PgStatSlruRow](pgPool, ctx, pgStatSlruQuery, "pg_stat_slru")
}

func NewPgStatSlruCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatSlruName,
		Interval: PgStatSlruInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatSlru(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatSlruPayload{Rows: rows}, nil
		},
	}
}
