package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SUBSCRIPTION

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatSubscriptionRow represents a row from pg_stat_subscription.
type PgStatSubscriptionRow struct {
	SubID              *int64  `json:"subid" db:"subid"`                                 // pg: oid
	SubName            *string `json:"subname" db:"subname"`                             // pg: name
	PID                *int64  `json:"pid" db:"pid"`                                     // pg: integer
	LeaderPID          *int64  `json:"leader_pid" db:"leader_pid"`                       // pg: integer
	RelID              *int64  `json:"relid" db:"relid"`                                 // pg: oid
	ReceivedLsn        *string `json:"received_lsn" db:"received_lsn"`                  // pg: pg_lsn
	LastMsgSendTime    *string `json:"last_msg_send_time" db:"last_msg_send_time"`       // pg: timestamp with time zone
	LastMsgReceiptTime *string `json:"last_msg_receipt_time" db:"last_msg_receipt_time"` // pg: timestamp with time zone
	LatestEndLsn       *string `json:"latest_end_lsn" db:"latest_end_lsn"`              // pg: pg_lsn
	LatestEndTime      *string `json:"latest_end_time" db:"latest_end_time"`             // pg: timestamp with time zone
	WorkerType         *string `json:"worker_type" db:"worker_type"`                     // pg: text
}

type PgStatSubscriptionPayload struct {
	Rows []PgStatSubscriptionRow `json:"rows"`
}

const (
	PgStatSubscriptionName     = "pg_stat_subscription"
	PgStatSubscriptionInterval = 1 * time.Minute
)

const pgStatSubscriptionQuery = `SELECT * FROM pg_stat_subscription`

func CollectPgStatSubscription(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatSubscriptionRow, error) {
	return CollectView[PgStatSubscriptionRow](pgPool, ctx, pgStatSubscriptionQuery, "pg_stat_subscription")
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
			return &PgStatSubscriptionPayload{Rows: rows}, nil
		},
	}
}
