package queries

// https://www.postgresql.org/docs/current/view-pg-replication-slots.html

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgReplicationSlotsName     = "pg_replication_slots"
	PgReplicationSlotsInterval = 1 * time.Minute
)

const pgReplicationSlotsQuery = `SELECT * FROM pg_replication_slots`

// PgReplicationSlotsRow represents a row from pg_replication_slots.
type PgReplicationSlotsRow struct {
	SlotName           *Name    `json:"slot_name" db:"slot_name"`
	Plugin             *Name    `json:"plugin" db:"plugin"`
	SlotType           *Text    `json:"slot_type" db:"slot_type"`
	DatOID             *Oid     `json:"datoid" db:"datoid"`
	Database           *Name    `json:"database" db:"database"`
	Temporary          *Boolean `json:"temporary" db:"temporary"`
	Active             *Boolean `json:"active" db:"active"`
	ActivePID          *Integer `json:"active_pid" db:"active_pid"`
	Xmin               *Xid     `json:"xmin" db:"xmin"`
	CatalogXmin        *Xid     `json:"catalog_xmin" db:"catalog_xmin"`
	RestartLsn         *PgLsn   `json:"restart_lsn" db:"restart_lsn"`
	ConfirmedFlushLsn  *PgLsn   `json:"confirmed_flush_lsn" db:"confirmed_flush_lsn"`
	WalStatus          *Text    `json:"wal_status" db:"wal_status"`
	SafeWalSize        *Bigint  `json:"safe_wal_size" db:"safe_wal_size"`
	TwoPhase           *Boolean `json:"two_phase" db:"two_phase"`
	Conflicting        *Boolean `json:"conflicting" db:"conflicting"`
	InvalidationReason *Text    `json:"invalidation_reason" db:"invalidation_reason"`
}

type PgReplicationSlotsPayload struct {
	Rows []PgReplicationSlotsRow `json:"rows"`
}

func CollectPgReplicationSlots(pgPool *pgxpool.Pool, ctx context.Context) ([]PgReplicationSlotsRow, error) {
	return CollectView[PgReplicationSlotsRow](pgPool, ctx, pgReplicationSlotsQuery, "pg_replication_slots")
}

func PgReplicationSlotsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
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
