package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// conninfo omitted (may contain passwords). sender_host + sender_port suffice.
// written_lsn/flushed_lsn are PG15+, use to_jsonb to safely extract.
const pgStatWalReceiverQuery = `
SELECT
    pid, status,
    receive_start_lsn::text AS receive_start_lsn,
    receive_start_tli,
    (to_jsonb(r) ->> 'written_lsn') AS written_lsn,
    (to_jsonb(r) ->> 'flushed_lsn') AS flushed_lsn,
    received_tli,
    last_msg_send_time::text AS last_msg_send_time,
    last_msg_receipt_time::text AS last_msg_receipt_time,
    latest_end_lsn::text AS latest_end_lsn,
    latest_end_time::text AS latest_end_time,
    slot_name,
    sender_host,
    sender_port
FROM pg_stat_wal_receiver r
`

func CollectPgStatWalReceiver(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatWalReceiverRow, error) {
	return CollectView[agent.PgStatWalReceiverRow](pgPool, ctx, pgStatWalReceiverQuery, "pg_stat_wal_receiver")
}
