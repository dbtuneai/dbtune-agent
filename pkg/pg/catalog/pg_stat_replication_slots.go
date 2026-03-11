package catalog

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
const pgStatReplicationSlotsQuery = `
SELECT
    slot_name,
    spill_txns, spill_count, spill_bytes,
    stream_txns, stream_count, stream_bytes,
    total_txns, total_bytes,
    stats_reset::text AS stats_reset
FROM pg_stat_replication_slots
`

func CollectPgStatReplicationSlots(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatReplicationSlotsRow, error) {
	if pgMajorVersion < 14 {
		return nil, nil
	}
	return CollectView[agent.PgStatReplicationSlotsRow](pgPool, ctx, pgStatReplicationSlotsQuery, "pg_stat_replication_slots")
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
			return &agent.PgStatReplicationSlotsPayload{Rows: rows}, nil
		},
	}
}
