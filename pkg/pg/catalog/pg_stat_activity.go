package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgStatActivityQuery = `
SELECT
    datid::bigint AS datid, datname, pid, leader_pid,
    usesysid::bigint AS usesysid, usename,
    application_name,
    client_addr::text AS client_addr,
    client_hostname, client_port,
    backend_start::text AS backend_start,
    xact_start::text AS xact_start,
    query_start::text AS query_start,
    state_change::text AS state_change,
    wait_event_type, wait_event, state,
    backend_xid::text AS backend_xid,
    backend_xmin::text AS backend_xmin,
    (to_jsonb(a) ->> 'query_id')::bigint AS query_id,
    query, backend_type
FROM pg_stat_activity a
`

func CollectPgStatActivity(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatActivityRow, error) {
	return CollectView[agent.PgStatActivityRow](pgPool, ctx, pgStatActivityQuery, "pg_stat_activity")
}
