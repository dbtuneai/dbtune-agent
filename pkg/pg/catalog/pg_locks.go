package catalog

// https://www.postgresql.org/docs/current/view-pg-locks.html

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgLocksName     = "pg_locks"
	PgLocksInterval = 30 * time.Second
)

// Filtered to only blocked locks + their blockers via pg_blocking_pids().
const pgLocksQuery = `
WITH blocked AS (
    SELECT pid AS blocked_pid, unnest(pg_blocking_pids(pid)) AS blocking_pid
    FROM pg_locks
    WHERE NOT granted
)
SELECT l.*
FROM pg_locks l
WHERE l.pid IN (SELECT blocked_pid FROM blocked UNION SELECT blocking_pid FROM blocked)
  AND l.database IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
`

// PgLocksRow represents a filtered row from pg_locks (blocked + blockers only).
type PgLocksRow struct {
	LockType           *Text        `json:"locktype" db:"locktype"`
	Database           *Oid         `json:"database" db:"database"`
	Relation           *Oid         `json:"relation" db:"relation"`
	Page               *Integer     `json:"page" db:"page"`
	Tuple              *Smallint    `json:"tuple" db:"tuple"`
	VirtualXID         *Text        `json:"virtualxid" db:"virtualxid"`
	TransactionID      *Xid         `json:"transactionid" db:"transactionid"`
	ClassID            *Oid         `json:"classid" db:"classid"`
	ObjID              *Oid         `json:"objid" db:"objid"`
	ObjSubID           *Smallint    `json:"objsubid" db:"objsubid"`
	VirtualTransaction *Text        `json:"virtualtransaction" db:"virtualtransaction"`
	PID                *Integer     `json:"pid" db:"pid"`
	Mode               *Text        `json:"mode" db:"mode"`
	Granted            *Boolean     `json:"granted" db:"granted"`
	FastPath           *Boolean     `json:"fastpath" db:"fastpath"`
	WaitStart          *TimestampTZ `json:"waitstart" db:"waitstart"`
}

type PgLocksPayload struct {
	Rows []PgLocksRow `json:"rows"`
}

func CollectPgLocks(pgPool *pgxpool.Pool, ctx context.Context) ([]PgLocksRow, error) {
	return CollectView[PgLocksRow](pgPool, ctx, pgLocksQuery, "pg_locks")
}

func NewPgLocksCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgLocksName,
		Interval: PgLocksInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgLocks(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgLocksPayload{Rows: rows}, nil
		},
	}
}
