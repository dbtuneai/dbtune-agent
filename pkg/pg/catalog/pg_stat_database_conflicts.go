package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
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
