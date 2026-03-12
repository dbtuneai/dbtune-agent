package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-USER-FUNCTIONS

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
SELECT * FROM pg_stat_user_functions
WHERE calls > 0
ORDER BY calls DESC
LIMIT 500
`

func CollectPgStatUserFunctions(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatUserFunctionsRow, error) {
	return CollectView[PgStatUserFunctionsRow](pgPool, ctx, pgStatUserFunctionsQuery, "pg_stat_user_functions")
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
			return &PgStatUserFunctionsPayload{Rows: rows}, nil
		},
	}
}

// PgStatUserFunctionsRow represents a row from pg_stat_user_functions.
type PgStatUserFunctionsRow struct {
	FuncID     *Oid             `json:"funcid" db:"funcid"`
	SchemaName *Name            `json:"schemaname" db:"schemaname"`
	FuncName   *Name            `json:"funcname" db:"funcname"`
	Calls      *Bigint          `json:"calls" db:"calls"`
	TotalTime  *DoublePrecision `json:"total_time" db:"total_time"`
	SelfTime   *DoublePrecision `json:"self_time" db:"self_time"`
}

type PgStatUserFunctionsPayload struct {
	Rows []PgStatUserFunctionsRow `json:"rows"`
}
