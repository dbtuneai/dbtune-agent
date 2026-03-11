package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgStatUserFunctionsQuery = `
SELECT funcid::bigint AS funcid, schemaname, funcname, calls, total_time, self_time
FROM pg_stat_user_functions
WHERE calls > 0
ORDER BY calls DESC
LIMIT 500
`

func CollectPgStatUserFunctions(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatUserFunctionsRow, error) {
	return CollectView[agent.PgStatUserFunctionsRow](pgPool, ctx, pgStatUserFunctionsQuery, "pg_stat_user_functions")
}
