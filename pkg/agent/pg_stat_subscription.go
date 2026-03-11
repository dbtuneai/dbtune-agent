package agent

// PgStatSubscriptionRow represents a row from pg_stat_subscription.
type PgStatSubscriptionRow struct {
	SubID              *int64  `json:"subid" db:"subid"`
	SubName            *string `json:"subname" db:"subname"`
	PID                *int64  `json:"pid" db:"pid"`
	LeaderPID          *int64  `json:"leader_pid" db:"leader_pid"`
	RelID              *int64  `json:"relid" db:"relid"`
	ReceivedLsn        *string `json:"received_lsn" db:"received_lsn"`
	LastMsgSendTime    *string `json:"last_msg_send_time" db:"last_msg_send_time"`
	LastMsgReceiptTime *string `json:"last_msg_receipt_time" db:"last_msg_receipt_time"`
	LatestEndLsn       *string `json:"latest_end_lsn" db:"latest_end_lsn"`
	LatestEndTime      *string `json:"latest_end_time" db:"latest_end_time"`
	WorkerType         *string `json:"worker_type" db:"worker_type"`
}

type PgStatSubscriptionPayload struct {
	Rows []PgStatSubscriptionRow `json:"rows"`
}
