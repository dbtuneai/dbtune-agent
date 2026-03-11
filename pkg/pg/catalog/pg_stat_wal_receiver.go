package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-WAL-RECEIVER

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatWalReceiverRow represents a row from pg_stat_wal_receiver (no conninfo).
type PgStatWalReceiverRow struct {
	PID                *int64  `json:"pid" db:"pid"`                                     // pg: integer
	Status             *string `json:"status" db:"status"`                               // pg: text
	ReceiveStartLsn    *string `json:"receive_start_lsn" db:"receive_start_lsn"`         // pg: pg_lsn
	ReceiveStartTli    *int64  `json:"receive_start_tli" db:"receive_start_tli"`         // pg: integer
	WrittenLsn         *string `json:"written_lsn" db:"written_lsn"`                    // pg: pg_lsn
	FlushedLsn         *string `json:"flushed_lsn" db:"flushed_lsn"`                    // pg: pg_lsn
	ReceivedTli        *int64  `json:"received_tli" db:"received_tli"`                   // pg: integer
	LastMsgSendTime    *string `json:"last_msg_send_time" db:"last_msg_send_time"`       // pg: timestamp with time zone
	LastMsgReceiptTime *string `json:"last_msg_receipt_time" db:"last_msg_receipt_time"` // pg: timestamp with time zone
	LatestEndLsn       *string `json:"latest_end_lsn" db:"latest_end_lsn"`              // pg: pg_lsn
	LatestEndTime      *string `json:"latest_end_time" db:"latest_end_time"`             // pg: timestamp with time zone
	SlotName           *string `json:"slot_name" db:"slot_name"`                         // pg: text
	SenderHost         *string `json:"sender_host" db:"sender_host"`                     // pg: text
	SenderPort         *int64  `json:"sender_port" db:"sender_port"`                     // pg: integer
}

type PgStatWalReceiverPayload struct {
	Rows []PgStatWalReceiverRow `json:"rows"`
}

const (
	PgStatWalReceiverName     = "pg_stat_wal_receiver"
	PgStatWalReceiverInterval = 1 * time.Minute
)

const pgStatWalReceiverQuery = `SELECT * FROM pg_stat_wal_receiver`

func CollectPgStatWalReceiver(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatWalReceiverRow, error) {
	return CollectView[PgStatWalReceiverRow](pgPool, ctx, pgStatWalReceiverQuery, "pg_stat_wal_receiver")
}

func NewPgStatWalReceiverCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatWalReceiverName,
		Interval: PgStatWalReceiverInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatWalReceiver(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatWalReceiverPayload{Rows: rows}, nil
		},
	}
}
