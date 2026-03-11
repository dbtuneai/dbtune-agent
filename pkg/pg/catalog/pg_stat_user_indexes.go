package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatUserIndexesName     = "pg_stat_user_indexes"
	PgStatUserIndexesInterval = 1 * time.Minute
)

const pgStatUserIndexesQuery = `SELECT * FROM pg_stat_user_indexes`

func CollectPgStatUserIndexes(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatUserIndexesRow, error) {
	return CollectView[PgStatUserIndexesRow](pgPool, ctx, pgStatUserIndexesQuery, "pg_stat_user_indexes")
}

func NewPgStatUserIndexesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatUserIndexesName,
		Interval: PgStatUserIndexesInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatUserIndexes(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatUserIndexesPayload{Rows: rows}, nil
		},
	}
}

// PgStatUserIndexesRow represents a row from pg_stat_user_indexes.
type PgStatUserIndexesRow struct {
	RelID        *int64  `json:"relid" db:"relid"`
	IndexRelID   *int64  `json:"indexrelid" db:"indexrelid"`
	SchemaName   *string `json:"schemaname" db:"schemaname"`
	RelName      *string `json:"relname" db:"relname"`
	IndexRelName *string `json:"indexrelname" db:"indexrelname"`
	IdxScan      *int64  `json:"idx_scan" db:"idx_scan"`
	LastIdxScan  *string `json:"last_idx_scan" db:"last_idx_scan"`
	IdxTupRead   *int64  `json:"idx_tup_read" db:"idx_tup_read"`
	IdxTupFetch  *int64  `json:"idx_tup_fetch" db:"idx_tup_fetch"`
}

type PgStatUserIndexesPayload struct {
	Rows []PgStatUserIndexesRow `json:"rows"`
}
