package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatUserFunctionsName     = "pg_stat_user_functions"
	PgStatUserFunctionsInterval = 1 * time.Minute
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

func NewPgStatUserFunctionsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatUserFunctionsName,
		Interval: PgStatUserFunctionsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatUserFunctions(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatUserFunctionsPayload{Rows: rows}, nil
		},
	}
}
