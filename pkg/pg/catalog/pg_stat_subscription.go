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
	SubID              *Oid         `json:"subid" db:"subid"`
	SubName            *Name        `json:"subname" db:"subname"`
	PID                *Integer     `json:"pid" db:"pid"`
	LeaderPID          *Integer     `json:"leader_pid" db:"leader_pid"`
	RelID              *Oid         `json:"relid" db:"relid"`
	ReceivedLsn        *PgLsn       `json:"received_lsn" db:"received_lsn"`
	LastMsgSendTime    *TimestampTZ `json:"last_msg_send_time" db:"last_msg_send_time"`
	LastMsgReceiptTime *TimestampTZ `json:"last_msg_receipt_time" db:"last_msg_receipt_time"`
	LatestEndLsn       *PgLsn       `json:"latest_end_lsn" db:"latest_end_lsn"`
	LatestEndTime      *TimestampTZ `json:"latest_end_time" db:"latest_end_time"`
	WorkerType         *Text        `json:"worker_type" db:"worker_type"`
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
