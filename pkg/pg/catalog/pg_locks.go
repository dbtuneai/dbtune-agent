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
	LockType           *string `json:"locktype" db:"locktype"`
	Database           *int64  `json:"database" db:"database"`
	Relation           *int64  `json:"relation" db:"relation"`
	Page               *int64  `json:"page" db:"page"`
	Tuple              *int64  `json:"tuple" db:"tuple"`
	VirtualXID         *string `json:"virtualxid" db:"virtualxid"`
	TransactionID      *string `json:"transactionid" db:"transactionid"`
	ClassID            *int64  `json:"classid" db:"classid"`
	ObjID              *int64  `json:"objid" db:"objid"`
	ObjSubID           *int64  `json:"objsubid" db:"objsubid"`
	VirtualTransaction *string `json:"virtualtransaction" db:"virtualtransaction"`
	PID                *int64  `json:"pid" db:"pid"`
	Mode               *string `json:"mode" db:"mode"`
	Granted            *bool   `json:"granted" db:"granted"`
	FastPath           *bool   `json:"fastpath" db:"fastpath"`
	WaitStart          *string `json:"waitstart" db:"waitstart"`
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
