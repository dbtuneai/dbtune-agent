package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatCheckpointerName     = "pg_stat_checkpointer"
	PgStatCheckpointerInterval = 1 * time.Minute
)

// PG 17+ only.
const pgStatCheckpointerQuery = `
SELECT
    num_timed, num_requested,
    restartpoints_timed, restartpoints_req, restartpoints_done,
    write_time, sync_time, buffers_written,
    stats_reset::text AS stats_reset,
    (to_jsonb(c) ->> 'slru_written')::bigint AS slru_written
FROM pg_stat_checkpointer c
`

func CollectPgStatCheckpointer(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatCheckpointerRow, error) {
	if pgMajorVersion < 17 {
		return nil, nil
	}
	return CollectView[agent.PgStatCheckpointerRow](pgPool, ctx, pgStatCheckpointerQuery, "pg_stat_checkpointer")
}

func NewPgStatCheckpointerCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatCheckpointerName,
		Interval: PgStatCheckpointerInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatCheckpointer(pool, ctx, pgMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &agent.PgStatCheckpointerPayload{Rows: rows}, nil
		},
	}
}
