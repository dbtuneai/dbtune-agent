package catalog

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
// waitstart is PG14+ so we use to_jsonb to safely extract it.
const pgLocksQuery = `
WITH blocked AS (
    SELECT pid AS blocked_pid, unnest(pg_blocking_pids(pid)) AS blocking_pid
    FROM pg_locks
    WHERE NOT granted
)
SELECT
    l.locktype,
    l.database,
    l.relation,
    l.page,
    l.tuple,
    l.virtualxid::text AS virtualxid,
    l.transactionid::text AS transactionid,
    l.classid,
    l.objid,
    l.objsubid,
    l.virtualtransaction,
    l.pid,
    l.mode,
    l.granted,
    l.fastpath,
    (to_jsonb(l) ->> 'waitstart') AS waitstart
FROM pg_locks l
WHERE l.pid IN (SELECT blocked_pid FROM blocked UNION SELECT blocking_pid FROM blocked)
`

func CollectPgLocks(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgLocksRow, error) {
	return CollectView[agent.PgLocksRow](pgPool, ctx, pgLocksQuery, "pg_locks")
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
			return &agent.PgLocksPayload{Rows: rows}, nil
		},
	}
}
