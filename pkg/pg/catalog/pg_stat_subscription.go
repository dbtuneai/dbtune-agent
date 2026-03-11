package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatSubscriptionName     = "pg_stat_subscription"
	PgStatSubscriptionInterval = 1 * time.Minute
)

// leader_pid is PG15+, worker_type is PG17+. Use to_jsonb to safely extract.
const pgStatSubscriptionQuery = `
SELECT
    subid, subname, pid,
    (to_jsonb(s) ->> 'leader_pid')::bigint AS leader_pid,
    relid,
    received_lsn::text AS received_lsn,
    last_msg_send_time::text AS last_msg_send_time,
    last_msg_receipt_time::text AS last_msg_receipt_time,
    latest_end_lsn::text AS latest_end_lsn,
    latest_end_time::text AS latest_end_time,
    (to_jsonb(s) ->> 'worker_type') AS worker_type
FROM pg_stat_subscription s
`

func CollectPgStatSubscription(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatSubscriptionRow, error) {
	return CollectView[agent.PgStatSubscriptionRow](pgPool, ctx, pgStatSubscriptionQuery, "pg_stat_subscription")
}

func NewPgStatSubscriptionCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatSubscriptionName,
		Interval: PgStatSubscriptionInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatSubscription(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatSubscriptionPayload{Rows: rows}, nil
		},
	}
}
