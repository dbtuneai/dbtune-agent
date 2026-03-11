package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PG 16+ only. Uses to_jsonb for op_bytes which was removed in PG 18.
const pgStatIOQuery = `
SELECT
    backend_type, object, context,
    reads, read_time, writes, write_time,
    writebacks, writeback_time,
    extends, extend_time,
    (to_jsonb(io) ->> 'op_bytes')::bigint AS op_bytes,
    hits, evictions, reuses,
    fsyncs, fsync_time,
    stats_reset::text AS stats_reset
FROM pg_stat_io io
`

func CollectPgStatIO(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatIORow, error) {
	if pgMajorVersion < 16 {
		return nil, nil
	}
	return CollectView[agent.PgStatIORow](pgPool, ctx, pgStatIOQuery, "pg_stat_io")
}
