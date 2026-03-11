package catalog

// https://www.postgresql.org/docs/current/view-pg-replication-slots.html

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

const pgReplicationSlotsQuery = `SELECT * FROM pg_replication_slots`

// PgReplicationSlotsRow represents a row from pg_replication_slots.
type PgReplicationSlotsRow struct {
	SlotName           *string `json:"slot_name" db:"slot_name"`                     // pg: name
	Plugin             *string `json:"plugin" db:"plugin"`                           // pg: name
	SlotType           *string `json:"slot_type" db:"slot_type"`                     // pg: text
	DatOID             *int64  `json:"datoid" db:"datoid"`                           // pg: oid
	Database           *string `json:"database" db:"database"`                       // pg: name
	Temporary          *bool   `json:"temporary" db:"temporary"`                     // pg: boolean
	Active             *bool   `json:"active" db:"active"`                           // pg: boolean
	ActivePID          *int64  `json:"active_pid" db:"active_pid"`                   // pg: integer
	Xmin               *string `json:"xmin" db:"xmin"`                               // pg: xid
	CatalogXmin        *string `json:"catalog_xmin" db:"catalog_xmin"`               // pg: xid
	RestartLsn         *string `json:"restart_lsn" db:"restart_lsn"`                 // pg: pg_lsn
	ConfirmedFlushLsn  *string `json:"confirmed_flush_lsn" db:"confirmed_flush_lsn"` // pg: pg_lsn
	WalStatus          *string `json:"wal_status" db:"wal_status"`                   // pg: text
	SafeWalSize        *int64  `json:"safe_wal_size" db:"safe_wal_size"`             // pg: bigint
	TwoPhase           *string `json:"two_phase" db:"two_phase"`                     // pg: boolean
	Conflicting        *string `json:"conflicting" db:"conflicting"`                 // pg: boolean
	InvalidationReason *string `json:"invalidation_reason" db:"invalidation_reason"` // pg: text
}

type PgReplicationSlotsPayload struct {
	Rows []PgReplicationSlotsRow `json:"rows"`
}

func CollectPgReplicationSlots(pgPool *pgxpool.Pool, ctx context.Context) ([]PgReplicationSlotsRow, error) {
	return CollectView[PgReplicationSlotsRow](pgPool, ctx, pgReplicationSlotsQuery, "pg_replication_slots")
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
			return &PgReplicationSlotsPayload{Rows: rows}, nil
		},
	}
}
