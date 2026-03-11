package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-SLOTS

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatReplicationSlotsName     = "pg_stat_replication_slots"
	PgStatReplicationSlotsInterval = 1 * time.Minute
)

// PG 14+ only.
const pgStatReplicationSlotsQuery = `SELECT * FROM pg_stat_replication_slots`

func CollectPgStatReplicationSlots(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]PgStatReplicationSlotsRow, error) {
	if pgMajorVersion < 14 {
		return nil, nil
	}
	return CollectView[PgStatReplicationSlotsRow](pgPool, ctx, pgStatReplicationSlotsQuery, "pg_stat_replication_slots")
}

func NewPgStatReplicationSlotsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatReplicationSlotsName,
		Interval: PgStatReplicationSlotsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatReplicationSlots(pool, ctx, pgMajorVersion)
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

// PgStatReplicationSlotsRow represents a row from pg_stat_replication_slots (PG 14+).
type PgStatReplicationSlotsRow struct {
	SlotName    *string `json:"slot_name" db:"slot_name"`       // pg: name
	SpillTxns   *int64  `json:"spill_txns" db:"spill_txns"`    // pg: bigint
	SpillCount  *int64  `json:"spill_count" db:"spill_count"`  // pg: bigint
	SpillBytes  *int64  `json:"spill_bytes" db:"spill_bytes"`  // pg: bigint
	StreamTxns  *int64  `json:"stream_txns" db:"stream_txns"`  // pg: bigint
	StreamCount *int64  `json:"stream_count" db:"stream_count"` // pg: bigint
	StreamBytes *int64  `json:"stream_bytes" db:"stream_bytes"` // pg: bigint
	TotalTxns   *int64  `json:"total_txns" db:"total_txns"`    // pg: bigint
	TotalBytes  *int64  `json:"total_bytes" db:"total_bytes"`  // pg: bigint
	StatsReset  *string `json:"stats_reset" db:"stats_reset"`  // pg: timestamp with time zone
}

type PgStatReplicationSlotsPayload struct {
	Rows []PgStatReplicationSlotsRow `json:"rows"`
}
