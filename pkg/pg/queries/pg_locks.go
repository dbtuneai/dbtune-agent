package queries

// https://www.postgresql.org/docs/current/view-pg-locks.html

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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

func PgLocksCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgLocksRow](pool, prepareCtx, PgLocksName, PgLocksInterval, pgLocksQuery)
}
