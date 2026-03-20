package queries

// https://www.postgresql.org/docs/current/view-pg-prepared-xacts.html

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgPreparedXactsRow represents a row from pg_prepared_xacts.
type PgPreparedXactsRow struct {
	Transaction *Xid         `json:"transaction" db:"transaction"`
	GID         *Text        `json:"gid" db:"gid"`
	Prepared    *TimestampTZ `json:"prepared" db:"prepared"`
	Owner       *Name        `json:"owner" db:"owner"`
	Database    *Name        `json:"database" db:"database"`
}

type PgPreparedXactsPayload struct {
	Rows []PgPreparedXactsRow `json:"rows"`
}

const (
	PgPreparedXactsName     = "pg_prepared_xacts"
	PgPreparedXactsInterval = 1 * time.Minute
)

const pgPreparedXactsQuery = `SELECT * FROM pg_prepared_xacts WHERE database = current_database() ORDER BY gid`

func CollectPgPreparedXacts(pgPool *pgxpool.Pool, ctx context.Context, scanner *pgxutil.Scanner[PgPreparedXactsRow]) ([]PgPreparedXactsRow, error) {
	return CollectView(pgPool, ctx, pgPreparedXactsQuery, "pg_prepared_xacts", scanner)
}

func PgPreparedXactsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	scanner := pgxutil.NewScanner[PgPreparedXactsRow]()
	return CatalogCollector{
		Name:          PgPreparedXactsName,
		Interval:      PgPreparedXactsInterval,
		SkipUnchanged: true,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgPreparedXacts(pool, ctx, scanner)
			if err != nil {
				return nil, err
			}
			return &PgPreparedXactsPayload{Rows: rows}, nil
		},
	}
}
