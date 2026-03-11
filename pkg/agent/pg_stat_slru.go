package agent

// PgStatSlruRow represents a row from pg_stat_slru.
type PgStatSlruRow struct {
	Name        *string `json:"name" db:"name"`
	BlksZeroed  *int64  `json:"blks_zeroed" db:"blks_zeroed"`
	BlksHit     *int64  `json:"blks_hit" db:"blks_hit"`
	BlksRead    *int64  `json:"blks_read" db:"blks_read"`
	BlksWritten *int64  `json:"blks_written" db:"blks_written"`
	BlksExists  *int64  `json:"blks_exists" db:"blks_exists"`
	Flushes     *int64  `json:"flushes" db:"flushes"`
	Truncates   *int64  `json:"truncates" db:"truncates"`
	StatsReset  *string `json:"stats_reset" db:"stats_reset"`
}

type PgStatSlruPayload struct {
	Rows []PgStatSlruRow `json:"rows"`
}
