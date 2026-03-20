package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-WAL

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatWalRow represents a row from pg_stat_wal (PG 14+).
type PgStatWalRow struct {
	WalRecords     *Bigint          `json:"wal_records" db:"wal_records"`
	WalFpi         *Bigint          `json:"wal_fpi" db:"wal_fpi"`
	WalBytes       *Numeric         `json:"wal_bytes" db:"wal_bytes"`
	WalBuffersFull *Bigint          `json:"wal_buffers_full" db:"wal_buffers_full"`
	WalWrite       *Bigint          `json:"wal_write" db:"wal_write"`
	WalSync        *Bigint          `json:"wal_sync" db:"wal_sync"`
	WalWriteTime   *DoublePrecision `json:"wal_write_time" db:"wal_write_time"`
	WalSyncTime    *DoublePrecision `json:"wal_sync_time" db:"wal_sync_time"`
	StatsReset     *TimestampTZ     `json:"stats_reset" db:"stats_reset"`
}

type PgStatWalPayload struct {
	Rows []PgStatWalRow `json:"rows"`
}

const (
	PgStatWalName     = "pg_stat_wal"
	PgStatWalInterval = 1 * time.Minute
)

// PG 14+ only.
const pgStatWalQuery = `SELECT * FROM pg_stat_wal`

func CollectPgStatWal(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int, scanner *pgxutil.Scanner[PgStatWalRow]) ([]PgStatWalRow, error) {
	if pgMajorVersion < 14 {
		return nil, nil
	}
	return CollectView(pgPool, ctx, pgStatWalQuery, "pg_stat_wal", scanner)
}

func PgStatWalCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	scanner := pgxutil.NewScanner[PgStatWalRow]()
	return CatalogCollector{
		Name:     PgStatWalName,
		Interval: PgStatWalInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatWal(pool, ctx, pgMajorVersion, scanner)
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
