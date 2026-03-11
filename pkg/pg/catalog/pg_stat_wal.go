package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-WAL

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatWalName     = "pg_stat_wal"
	PgStatWalInterval = 1 * time.Minute
)

// PG 14+ only.
const pgStatWalQuery = `SELECT * FROM pg_stat_wal`

func CollectPgStatWal(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]PgStatWalRow, error) {
	if pgMajorVersion < 14 {
		return nil, nil
	}
	return CollectView[PgStatWalRow](pgPool, ctx, pgStatWalQuery, "pg_stat_wal")
}

func NewPgStatWalCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatWalName,
		Interval: PgStatWalInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatWal(pool, ctx, pgMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &PgStatWalPayload{Rows: rows}, nil
		},
	}
}

// PgStatWalRow represents a row from pg_stat_wal (PG 14+).
type PgStatWalRow struct {
	WalRecords     *int64   `json:"wal_records" db:"wal_records"`           // pg: bigint
	WalFpi         *int64   `json:"wal_fpi" db:"wal_fpi"`                   // pg: bigint
	WalBytes       *int64   `json:"wal_bytes" db:"wal_bytes"`               // pg: numeric
	WalBuffersFull *int64   `json:"wal_buffers_full" db:"wal_buffers_full"` // pg: bigint
	WalWrite       *int64   `json:"wal_write" db:"wal_write"`               // pg: bigint
	WalSync        *int64   `json:"wal_sync" db:"wal_sync"`                 // pg: bigint
	WalWriteTime   *float64 `json:"wal_write_time" db:"wal_write_time"`     // pg: double precision
	WalSyncTime    *float64 `json:"wal_sync_time" db:"wal_sync_time"`       // pg: double precision
	StatsReset     *string  `json:"stats_reset" db:"stats_reset"`           // pg: timestamp with time zone
}

type PgStatWalPayload struct {
	Rows []PgStatWalRow `json:"rows"`
}
