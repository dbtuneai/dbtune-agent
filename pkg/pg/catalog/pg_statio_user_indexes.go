package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatioUserIndexesName     = "pg_statio_user_indexes"
	PgStatioUserIndexesInterval = 1 * time.Minute
)

const pgStatioUserIndexesQuery = `
SELECT * FROM pg_statio_user_indexes
WHERE COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0) > 0
`

func CollectPgStatioUserIndexes(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatioUserIndexesRow, error) {
	return CollectView[PgStatioUserIndexesRow](pgPool, ctx, pgStatioUserIndexesQuery, "pg_statio_user_indexes")
}

func NewPgStatioUserIndexesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatioUserIndexesName,
		Interval: PgStatioUserIndexesInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatioUserIndexes(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatioUserIndexesPayload{Rows: rows}, nil
		},
	}
}

// PgStatioUserIndexesRow represents a row from pg_statio_user_indexes.
type PgStatioUserIndexesRow struct {
	RelID        *int64  `json:"relid" db:"relid"`
	IndexRelID   *int64  `json:"indexrelid" db:"indexrelid"`
	SchemaName   *string `json:"schemaname" db:"schemaname"`
	RelName      *string `json:"relname" db:"relname"`
	IndexRelName *string `json:"indexrelname" db:"indexrelname"`
	IdxBlksRead  *int64  `json:"idx_blks_read" db:"idx_blks_read"`
	IdxBlksHit   *int64  `json:"idx_blks_hit" db:"idx_blks_hit"`
}

type PgStatioUserIndexesPayload struct {
	Rows []PgStatioUserIndexesRow `json:"rows"`
}
