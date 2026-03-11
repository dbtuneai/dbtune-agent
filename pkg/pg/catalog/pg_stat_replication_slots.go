package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
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
