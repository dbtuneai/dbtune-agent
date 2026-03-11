package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatDatabaseName     = "pg_stat_database"
	PgStatDatabaseInterval = 1 * time.Minute
)

const pgStatDatabaseQuery = `
SELECT
    datid::bigint AS datid, datname, numbackends,
    xact_commit, xact_rollback, blks_read, blks_hit,
    tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
    conflicts, temp_files, temp_bytes, deadlocks,
    checksum_failures,
    checksum_last_failure::text AS checksum_last_failure,
    blk_read_time, blk_write_time,
    (to_jsonb(d) ->> 'session_time')::float8 AS session_time,
    (to_jsonb(d) ->> 'active_time')::float8 AS active_time,
    (to_jsonb(d) ->> 'idle_in_transaction_time')::float8 AS idle_in_transaction_time,
    (to_jsonb(d) ->> 'sessions')::bigint AS sessions,
    (to_jsonb(d) ->> 'sessions_abandoned')::bigint AS sessions_abandoned,
    (to_jsonb(d) ->> 'sessions_fatal')::bigint AS sessions_fatal,
    (to_jsonb(d) ->> 'sessions_killed')::bigint AS sessions_killed,
    stats_reset::text AS stats_reset,
    (to_jsonb(d) ->> 'parallel_workers')::bigint AS parallel_workers,
    (to_jsonb(d) ->> 'temp_bytes_read')::bigint AS temp_bytes_read,
    (to_jsonb(d) ->> 'temp_bytes_written')::bigint AS temp_bytes_written,
    (to_jsonb(d) ->> 'temp_files_read')::bigint AS temp_files_read,
    (to_jsonb(d) ->> 'temp_files_written')::bigint AS temp_files_written
FROM pg_stat_database d
`

func CollectPgStatDatabase(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatDatabaseRow, error) {
	return CollectView[agent.PgStatDatabaseRow](pgPool, ctx, pgStatDatabaseQuery, "pg_stat_database")
}

func NewPgStatDatabaseCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatDatabaseName,
		Interval: PgStatDatabaseInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatDatabase(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatDatabasePayload{Rows: rows}, nil
		},
	}
}
