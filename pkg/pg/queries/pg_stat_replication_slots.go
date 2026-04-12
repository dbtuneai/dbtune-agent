package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-SLOTS

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatReplicationSlotsRow represents a row from pg_stat_replication_slots (PG 14+).
type PgStatReplicationSlotsRow struct {
	SlotName    *Name        `json:"slot_name" db:"slot_name"`
	SpillTxns   *Bigint      `json:"spill_txns" db:"spill_txns"`
	SpillCount  *Bigint      `json:"spill_count" db:"spill_count"`
	SpillBytes  *Bigint      `json:"spill_bytes" db:"spill_bytes"`
	StreamTxns  *Bigint      `json:"stream_txns" db:"stream_txns"`
	StreamCount *Bigint      `json:"stream_count" db:"stream_count"`
	StreamBytes *Bigint      `json:"stream_bytes" db:"stream_bytes"`
	TotalTxns   *Bigint      `json:"total_txns" db:"total_txns"`
	TotalBytes  *Bigint      `json:"total_bytes" db:"total_bytes"`
	StatsReset  *TimestampTZ `json:"stats_reset" db:"stats_reset"`
}

const (
	PgStatReplicationSlotsName     = "pg_stat_replication_slots"
	PgStatReplicationSlotsInterval = 1 * time.Minute
)

// PG 14+ only.
const pgStatReplicationSlotsQuery = `SELECT * FROM pg_stat_replication_slots`

// PgStatReplicationSlotsRegistration describes the pgstatreplicationslots collector's configuration schema.
var PgStatReplicationSlotsRegistration = collectorconfig.CollectorRegistration{
	Name: PgStatReplicationSlotsName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgStatReplicationSlotsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	return NewCollector[PgStatReplicationSlotsRow](pool, prepareCtx, PgStatReplicationSlotsName, PgStatReplicationSlotsInterval, pgStatReplicationSlotsQuery, WithMinPGVersion(pgMajorVersion, 14))
}
