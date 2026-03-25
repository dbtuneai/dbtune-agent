package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ARCHIVER

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatArchiverRow represents a row from pg_stat_archiver.
type PgStatArchiverRow struct {
	ArchivedCount    *Bigint      `json:"archived_count" db:"archived_count"`
	LastArchivedWal  *Text        `json:"last_archived_wal" db:"last_archived_wal"`
	LastArchivedTime *TimestampTZ `json:"last_archived_time" db:"last_archived_time"`
	FailedCount      *Bigint      `json:"failed_count" db:"failed_count"`
	LastFailedWal    *Text        `json:"last_failed_wal" db:"last_failed_wal"`
	LastFailedTime   *TimestampTZ `json:"last_failed_time" db:"last_failed_time"`
	StatsReset       *TimestampTZ `json:"stats_reset" db:"stats_reset"`
}

const (
	PgStatArchiverName     = "pg_stat_archiver"
	PgStatArchiverInterval = 1 * time.Minute
)

const pgStatArchiverQuery = `SELECT * FROM pg_stat_archiver`

func PgStatArchiverCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgStatArchiverRow](pool, prepareCtx, PgStatArchiverName, PgStatArchiverInterval, pgStatArchiverQuery, WithSkipUnchanged())
}
