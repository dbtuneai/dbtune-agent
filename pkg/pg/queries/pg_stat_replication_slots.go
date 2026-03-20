package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-SLOTS

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
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

type PgStatReplicationSlotsPayload struct {
	Rows []PgStatReplicationSlotsRow `json:"rows"`
}

const (
	PgStatReplicationSlotsName     = "pg_stat_replication_slots"
	PgStatReplicationSlotsInterval = 1 * time.Minute
)

// PG 14+ only.
const pgStatReplicationSlotsQuery = `SELECT * FROM pg_stat_replication_slots`

func CollectPgStatReplicationSlots(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int, scanner *pgxutil.Scanner[PgStatReplicationSlotsRow]) ([]PgStatReplicationSlotsRow, error) {
	if pgMajorVersion < 14 {
		return nil, nil
	}
	return CollectView(pgPool, ctx, pgStatReplicationSlotsQuery, "pg_stat_replication_slots", scanner)
}

func PgStatReplicationSlotsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	scanner := pgxutil.NewScanner[PgStatReplicationSlotsRow]()
	return CatalogCollector{
		Name:     PgStatReplicationSlotsName,
		Interval: PgStatReplicationSlotsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatReplicationSlots(pool, ctx, pgMajorVersion, scanner)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &PgStatReplicationSlotsPayload{Rows: rows}, nil
		},
	}
}
