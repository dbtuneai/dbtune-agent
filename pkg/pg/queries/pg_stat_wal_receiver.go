package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-WAL-RECEIVER

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatWalReceiverRow represents a row from pg_stat_wal_receiver (no conninfo).
type PgStatWalReceiverRow struct {
	PID                *Integer     `json:"pid" db:"pid"`
	Status             *Text        `json:"status" db:"status"`
	ReceiveStartLsn    *PgLsn       `json:"receive_start_lsn" db:"receive_start_lsn"`
	ReceiveStartTli    *Integer     `json:"receive_start_tli" db:"receive_start_tli"`
	WrittenLsn         *PgLsn       `json:"written_lsn" db:"written_lsn"`
	FlushedLsn         *PgLsn       `json:"flushed_lsn" db:"flushed_lsn"`
	ReceivedTli        *Integer     `json:"received_tli" db:"received_tli"`
	LastMsgSendTime    *TimestampTZ `json:"last_msg_send_time" db:"last_msg_send_time"`
	LastMsgReceiptTime *TimestampTZ `json:"last_msg_receipt_time" db:"last_msg_receipt_time"`
	LatestEndLsn       *PgLsn       `json:"latest_end_lsn" db:"latest_end_lsn"`
	LatestEndTime      *TimestampTZ `json:"latest_end_time" db:"latest_end_time"`
	SlotName           *Text        `json:"slot_name" db:"slot_name"`
	SenderHost         *Text        `json:"sender_host" db:"sender_host"`
	SenderPort         *Integer     `json:"sender_port" db:"sender_port"`
}

const (
	PgStatWalReceiverName     = "pg_stat_wal_receiver"
	PgStatWalReceiverInterval = 1 * time.Minute
)

const pgStatWalReceiverQuery = `SELECT * FROM pg_stat_wal_receiver`

// PgStatWalReceiverRegistration describes the pgstatwalreceiver collector's configuration schema.
var PgStatWalReceiverRegistration = collectorconfig.CollectorRegistration{
	Name: PgStatWalReceiverName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgStatWalReceiverCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgStatWalReceiverRow](pool, prepareCtx, PgStatWalReceiverName, PgStatWalReceiverInterval, pgStatWalReceiverQuery)
}
