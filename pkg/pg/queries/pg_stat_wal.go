package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-WAL

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatWalRow represents a row from pg_stat_wal (PG 14+).
type PgStatWalRow struct {
	WalRecords     *Bigint          `json:"wal_records" db:"wal_records"`
	WalFpi         *Bigint          `json:"wal_fpi" db:"wal_fpi"`
	WalBytes       *Bigint          `json:"wal_bytes" db:"wal_bytes"` // pg numeric, but int64 suffices (~9.2 EB max)
	WalBuffersFull *Bigint          `json:"wal_buffers_full" db:"wal_buffers_full"`
	WalWrite       *Bigint          `json:"wal_write" db:"wal_write"`
	WalSync        *Bigint          `json:"wal_sync" db:"wal_sync"`
	WalWriteTime   *DoublePrecision `json:"wal_write_time" db:"wal_write_time"`
	WalSyncTime    *DoublePrecision `json:"wal_sync_time" db:"wal_sync_time"`
	StatsReset     *TimestampTZ     `json:"stats_reset" db:"stats_reset"`
}

const (
	PgStatWalName     = "pg_stat_wal"
	PgStatWalInterval = 1 * time.Minute
)

// PG 14+ only.
const pgStatWalQuery = `SELECT * FROM pg_stat_wal`

// PgStatWalRegistration describes the pgstatwal collector's configuration schema.
var PgStatWalRegistration = collectorconfig.CollectorRegistration{
	Name: PgStatWalName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgStatWalCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	return NewCollector[PgStatWalRow](pool, prepareCtx, PgStatWalName, PgStatWalInterval, pgStatWalQuery, WithMinPGVersion(pgMajorVersion, 14))
}
