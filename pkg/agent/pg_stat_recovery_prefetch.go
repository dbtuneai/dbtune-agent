package agent

// PgStatRecoveryPrefetchRow represents a row from pg_stat_recovery_prefetch (PG 15+).
type PgStatRecoveryPrefetchRow struct {
	StatsReset    *string `json:"stats_reset" db:"stats_reset"`
	Prefetch      *int64  `json:"prefetch" db:"prefetch"`
	Hit           *int64  `json:"hit" db:"hit"`
	SkipInit      *int64  `json:"skip_init" db:"skip_init"`
	SkipNew       *int64  `json:"skip_new" db:"skip_new"`
	SkipFpw       *int64  `json:"skip_fpw" db:"skip_fpw"`
	SkipRep       *int64  `json:"skip_rep" db:"skip_rep"`
	WalDistance   *int64  `json:"wal_distance" db:"wal_distance"`
	BlockDistance *int64  `json:"block_distance" db:"block_distance"`
	IoDepth       *int64  `json:"io_depth" db:"io_depth"`
}

type PgStatRecoveryPrefetchPayload struct {
	Rows []PgStatRecoveryPrefetchRow `json:"rows"`
}
