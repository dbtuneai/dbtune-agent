package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatDatabaseConflictsName     = "pg_stat_database_conflicts"
	PgStatDatabaseConflictsInterval = 1 * time.Minute
)

const pgStatDatabaseConflictsQuery = `
SELECT
    datid::bigint AS datid, datname,
    confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock,
    (to_jsonb(c) ->> 'confl_active_logicalslot')::bigint AS confl_active_logicalslot
FROM pg_stat_database_conflicts c
`

func CollectPgStatDatabaseConflicts(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatDatabaseConflictsRow, error) {
	return CollectView[agent.PgStatDatabaseConflictsRow](pgPool, ctx, pgStatDatabaseConflictsQuery, "pg_stat_database_conflicts")
}

func NewPgStatDatabaseConflictsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatDatabaseConflictsName,
		Interval: PgStatDatabaseConflictsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatDatabaseConflicts(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatDatabaseConflictsPayload{Rows: rows}, nil
		},
	}
}
