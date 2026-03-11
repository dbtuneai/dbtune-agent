package agent

// PgStatWalRow represents a row from pg_stat_wal (PG 14+).
type PgStatWalRow struct {
	WalRecords     *int64   `json:"wal_records" db:"wal_records"`
	WalFpi         *int64   `json:"wal_fpi" db:"wal_fpi"`
	WalBytes       *int64   `json:"wal_bytes" db:"wal_bytes"`
	WalBuffersFull *int64   `json:"wal_buffers_full" db:"wal_buffers_full"`
	WalWrite       *int64   `json:"wal_write" db:"wal_write"`
	WalSync        *int64   `json:"wal_sync" db:"wal_sync"`
	WalWriteTime   *float64 `json:"wal_write_time" db:"wal_write_time"`
	WalSyncTime    *float64 `json:"wal_sync_time" db:"wal_sync_time"`
	StatsReset     *string  `json:"stats_reset" db:"stats_reset"`
}

type PgStatWalPayload struct {
	Rows []PgStatWalRow `json:"rows"`
}
