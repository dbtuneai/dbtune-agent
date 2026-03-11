package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatBgwriterName     = "pg_stat_bgwriter"
	PgStatBgwriterInterval = 1 * time.Minute
)

// Uses to_jsonb trick for columns removed or moved in PG 17 (to pg_stat_checkpointer).
const pgStatBgwriterQuery = `
SELECT
    (to_jsonb(b) ->> 'checkpoints_timed')::bigint AS checkpoints_timed,
    (to_jsonb(b) ->> 'checkpoints_req')::bigint AS checkpoints_req,
    (to_jsonb(b) ->> 'checkpoint_write_time')::float8 AS checkpoint_write_time,
    (to_jsonb(b) ->> 'checkpoint_sync_time')::float8 AS checkpoint_sync_time,
    (to_jsonb(b) ->> 'buffers_checkpoint')::bigint AS buffers_checkpoint,
    buffers_clean, maxwritten_clean,
    (to_jsonb(b) ->> 'buffers_backend')::bigint AS buffers_backend,
    (to_jsonb(b) ->> 'buffers_backend_fsync')::bigint AS buffers_backend_fsync,
    buffers_alloc,
    stats_reset::text AS stats_reset
FROM pg_stat_bgwriter b
`

func CollectPgStatBgwriter(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatBgwriterRow, error) {
	return CollectView[agent.PgStatBgwriterRow](pgPool, ctx, pgStatBgwriterQuery, "pg_stat_bgwriter")
}

func NewPgStatBgwriterCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatBgwriterName,
		Interval: PgStatBgwriterInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatBgwriter(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatBgwriterPayload{Rows: rows}, nil
		},
	}
}
