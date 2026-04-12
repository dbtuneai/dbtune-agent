package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SUBSCRIPTION

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

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

const (
	PgStatSubscriptionName     = "pg_stat_subscription"
	PgStatSubscriptionInterval = 1 * time.Minute
)

const pgStatSubscriptionQuery = `SELECT * FROM pg_stat_subscription`

// PgStatSubscriptionRegistration describes the pgstatsubscription collector's configuration schema.
var PgStatSubscriptionRegistration = collectorconfig.CollectorRegistration{
	Name: PgStatSubscriptionName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgStatSubscriptionCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgStatSubscriptionRow](pool, prepareCtx, PgStatSubscriptionName, PgStatSubscriptionInterval, pgStatSubscriptionQuery)
}
