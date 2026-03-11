package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgPreparedXactsQuery = `
SELECT
    transaction::text AS transaction,
    gid,
    prepared::text AS prepared,
    owner,
    database
FROM pg_prepared_xacts
`

func CollectPgPreparedXacts(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgPreparedXactsRow, error) {
	return CollectView[agent.PgPreparedXactsRow](pgPool, ctx, pgPreparedXactsQuery, "pg_prepared_xacts")
}
