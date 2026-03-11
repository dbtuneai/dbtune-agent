package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgPreparedXactsName     = "pg_prepared_xacts"
	PgPreparedXactsInterval = 1 * time.Minute
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

func NewPgPreparedXactsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgPreparedXactsName,
		Interval: PgPreparedXactsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgPreparedXacts(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgPreparedXactsPayload{Rows: rows}, nil
		},
	}
}
