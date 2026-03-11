package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgStatReplicationQuery = `
SELECT
    pid, usesysid, usename, application_name,
    client_addr::text AS client_addr,
    client_hostname, client_port,
    backend_start::text AS backend_start,
    backend_xmin::text AS backend_xmin,
    state,
    sent_lsn::text AS sent_lsn,
    write_lsn::text AS write_lsn,
    flush_lsn::text AS flush_lsn,
    replay_lsn::text AS replay_lsn,
    write_lag::text AS write_lag,
    flush_lag::text AS flush_lag,
    replay_lag::text AS replay_lag,
    sync_priority, sync_state,
    reply_time::text AS reply_time
FROM pg_stat_replication
`

func CollectPgStatReplication(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatReplicationRow, error) {
	return CollectView[agent.PgStatReplicationRow](pgPool, ctx, pgStatReplicationQuery, "pg_stat_replication")
}
