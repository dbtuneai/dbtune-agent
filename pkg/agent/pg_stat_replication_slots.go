package agent

// PgStatReplicationSlotsRow represents a row from pg_stat_replication_slots (PG 14+).
type PgStatReplicationSlotsRow struct {
	SlotName   *string `json:"slot_name" db:"slot_name"`
	SpillTxns  *int64  `json:"spill_txns" db:"spill_txns"`
	SpillCount *int64  `json:"spill_count" db:"spill_count"`
	SpillBytes *int64  `json:"spill_bytes" db:"spill_bytes"`
	StreamTxns *int64  `json:"stream_txns" db:"stream_txns"`
	StreamCount *int64 `json:"stream_count" db:"stream_count"`
	StreamBytes *int64 `json:"stream_bytes" db:"stream_bytes"`
	TotalTxns  *int64  `json:"total_txns" db:"total_txns"`
	TotalBytes *int64  `json:"total_bytes" db:"total_bytes"`
	StatsReset *string `json:"stats_reset" db:"stats_reset"`
}

type PgStatReplicationSlotsPayload struct {
	Rows []PgStatReplicationSlotsRow `json:"rows"`
}
