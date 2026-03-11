package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatUserTablesName     = "pg_stat_user_tables"
	PgStatUserTablesInterval = 1 * time.Minute
)

const pgStatUserTablesQuery = `
SELECT
    relid::bigint AS relid, schemaname, relname,
    seq_scan,
    (to_jsonb(t) ->> 'last_seq_scan')::text AS last_seq_scan,
    seq_tup_read, idx_scan,
    (to_jsonb(t) ->> 'last_idx_scan')::text AS last_idx_scan,
    idx_tup_fetch,
    n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
    (to_jsonb(t) ->> 'n_tup_newpage_upd')::bigint AS n_tup_newpage_upd,
    n_live_tup, n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum,
    last_vacuum::text AS last_vacuum,
    last_autovacuum::text AS last_autovacuum,
    last_analyze::text AS last_analyze,
    last_autoanalyze::text AS last_autoanalyze,
    vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
    (to_jsonb(t) ->> 'total_vacuum_time')::float8 AS total_vacuum_time,
    (to_jsonb(t) ->> 'total_autovacuum_time')::float8 AS total_autovacuum_time,
    (to_jsonb(t) ->> 'total_analyze_time')::float8 AS total_analyze_time,
    (to_jsonb(t) ->> 'total_autoanalyze_time')::float8 AS total_autoanalyze_time
FROM pg_stat_user_tables t
ORDER BY schemaname, relname
`

func CollectPgStatUserTables(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatUserTableRow, error) {
	return CollectView[agent.PgStatUserTableRow](pgPool, ctx, pgStatUserTablesQuery, "pg_stat_user_tables")
}

func NewPgStatUserTablesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatUserTablesName,
		Interval: PgStatUserTablesInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatUserTables(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatUserTablePayload{Rows: rows}, nil
		},
	}
}
