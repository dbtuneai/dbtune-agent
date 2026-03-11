package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PG 14+ only. Uses to_jsonb for columns removed in PG 18
// (wal_records, wal_fpi, wal_buffers_full, wal_write, wal_sync, wal_write_time, wal_sync_time).
const pgStatWalQuery = `
SELECT
    (to_jsonb(w) ->> 'wal_records')::bigint AS wal_records,
    (to_jsonb(w) ->> 'wal_fpi')::bigint AS wal_fpi,
    wal_bytes::bigint AS wal_bytes,
    (to_jsonb(w) ->> 'wal_buffers_full')::bigint AS wal_buffers_full,
    (to_jsonb(w) ->> 'wal_write')::bigint AS wal_write,
    (to_jsonb(w) ->> 'wal_sync')::bigint AS wal_sync,
    (to_jsonb(w) ->> 'wal_write_time')::float8 AS wal_write_time,
    (to_jsonb(w) ->> 'wal_sync_time')::float8 AS wal_sync_time,
    stats_reset::text AS stats_reset
FROM pg_stat_wal w
`

func CollectPgStatWal(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatWalRow, error) {
	if pgMajorVersion < 14 {
		return nil, nil
	}
	return CollectView[agent.PgStatWalRow](pgPool, ctx, pgStatWalQuery, "pg_stat_wal")
}
