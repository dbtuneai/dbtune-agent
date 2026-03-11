package catalog

// https://www.postgresql.org/docs/current/view-pg-prepared-xacts.html

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

const pgPreparedXactsQuery = `SELECT * FROM pg_prepared_xacts WHERE database = current_database() ORDER BY gid`

// PgPreparedXactsRow represents a row from pg_prepared_xacts.
type PgPreparedXactsRow struct {
	Transaction *string `json:"transaction" db:"transaction"` // pg: xid
	GID         *string `json:"gid" db:"gid"`                 // pg: text
	Prepared    *string `json:"prepared" db:"prepared"`       // pg: timestamp with time zone
	Owner       *string `json:"owner" db:"owner"`             // pg: name
	Database    *string `json:"database" db:"database"`       // pg: name
}

type PgPreparedXactsPayload struct {
	Rows []PgPreparedXactsRow `json:"rows"`
}

func CollectPgPreparedXacts(pgPool *pgxpool.Pool, ctx context.Context) ([]PgPreparedXactsRow, error) {
	return CollectView[PgPreparedXactsRow](pgPool, ctx, pgPreparedXactsQuery, "pg_prepared_xacts")
}

func NewPgPreparedXactsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:          PgPreparedXactsName,
		Interval:      PgPreparedXactsInterval,
		SkipUnchanged: true,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgPreparedXacts(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgPreparedXactsPayload{Rows: rows}, nil
		},
	}
}
