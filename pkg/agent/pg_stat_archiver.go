package agent

// PgStatArchiverRow represents a row from pg_stat_archiver.
type PgStatArchiverRow struct {
	ArchivedCount    *int64  `json:"archived_count" db:"archived_count"`
	LastArchivedWal  *string `json:"last_archived_wal" db:"last_archived_wal"`
	LastArchivedTime *string `json:"last_archived_time" db:"last_archived_time"`
	FailedCount      *int64  `json:"failed_count" db:"failed_count"`
	LastFailedWal    *string `json:"last_failed_wal" db:"last_failed_wal"`
	LastFailedTime   *string `json:"last_failed_time" db:"last_failed_time"`
	StatsReset       *string `json:"stats_reset" db:"stats_reset"`
}

type PgStatArchiverPayload struct {
	Rows []PgStatArchiverRow `json:"rows"`
}
