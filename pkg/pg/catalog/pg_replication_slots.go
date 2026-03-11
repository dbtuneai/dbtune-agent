package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgReplicationSlotsName     = "pg_replication_slots"
	PgReplicationSlotsInterval = 1 * time.Minute
)

// two_phase/conflicting are PG15+, invalidation_reason is PG17+.
// Use to_jsonb to safely extract them on older versions.
const pgReplicationSlotsQuery = `
SELECT
    slot_name, plugin, slot_type,
    datoid, database,
    temporary, active, active_pid,
    xmin::text AS xmin,
    catalog_xmin::text AS catalog_xmin,
    restart_lsn::text AS restart_lsn,
    confirmed_flush_lsn::text AS confirmed_flush_lsn,
    wal_status, safe_wal_size,
    (to_jsonb(s) ->> 'two_phase') AS two_phase,
    (to_jsonb(s) ->> 'conflicting') AS conflicting,
    (to_jsonb(s) ->> 'invalidation_reason') AS invalidation_reason
FROM pg_replication_slots s
`

func CollectPgReplicationSlots(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgReplicationSlotsRow, error) {
	return CollectView[agent.PgReplicationSlotsRow](pgPool, ctx, pgReplicationSlotsQuery, "pg_replication_slots")
}

func NewPgReplicationSlotsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgReplicationSlotsName,
		Interval: PgReplicationSlotsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgReplicationSlots(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgReplicationSlotsPayload{Rows: rows}, nil
		},
	}
}
