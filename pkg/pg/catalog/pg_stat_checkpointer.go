package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
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
