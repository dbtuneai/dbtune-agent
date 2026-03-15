package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatDatabaseName     = "pg_stat_database"
	PgStatDatabaseInterval = 1 * time.Minute
)

const pgStatDatabaseQuery = `SELECT * FROM pg_stat_database WHERE datname = current_database()`

func CollectPgStatDatabase(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatDatabaseRow, error) {
	return CollectView[PgStatDatabaseRow](pgPool, ctx, pgStatDatabaseQuery, "pg_stat_database")
}

func PgStatDatabaseCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
		Name:     PgStatDatabaseName,
		Interval: PgStatDatabaseInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatDatabase(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatDatabasePayload{Rows: rows}, nil
		},
	}
}

// PgStatDatabaseRow represents a row from pg_stat_database.
type PgStatDatabaseRow struct {
	DatID                 *Oid             `json:"datid" db:"datid"`
	DatName               *Name            `json:"datname" db:"datname"`
	NumBackends           *Integer         `json:"numbackends" db:"numbackends"`
	XactCommit            *Bigint          `json:"xact_commit" db:"xact_commit"`
	XactRollback          *Bigint          `json:"xact_rollback" db:"xact_rollback"`
	BlksRead              *Bigint          `json:"blks_read" db:"blks_read"`
	BlksHit               *Bigint          `json:"blks_hit" db:"blks_hit"`
	TupReturned           *Bigint          `json:"tup_returned" db:"tup_returned"`
	TupFetched            *Bigint          `json:"tup_fetched" db:"tup_fetched"`
	TupInserted           *Bigint          `json:"tup_inserted" db:"tup_inserted"`
	TupUpdated            *Bigint          `json:"tup_updated" db:"tup_updated"`
	TupDeleted            *Bigint          `json:"tup_deleted" db:"tup_deleted"`
	Conflicts             *Bigint          `json:"conflicts" db:"conflicts"`
	TempFiles             *Bigint          `json:"temp_files" db:"temp_files"`
	TempBytes             *Bigint          `json:"temp_bytes" db:"temp_bytes"`
	Deadlocks             *Bigint          `json:"deadlocks" db:"deadlocks"`
	ChecksumFailures      *Bigint          `json:"checksum_failures" db:"checksum_failures"`
	ChecksumLastFailure   *TimestampTZ     `json:"checksum_last_failure" db:"checksum_last_failure"`
	BlkReadTime           *DoublePrecision `json:"blk_read_time" db:"blk_read_time"`
	BlkWriteTime          *DoublePrecision `json:"blk_write_time" db:"blk_write_time"`
	SessionTime           *DoublePrecision `json:"session_time" db:"session_time"`
	ActiveTime            *DoublePrecision `json:"active_time" db:"active_time"`
	IdleInTransactionTime *DoublePrecision `json:"idle_in_transaction_time" db:"idle_in_transaction_time"`
	Sessions              *Bigint          `json:"sessions" db:"sessions"`
	SessionsAbandoned     *Bigint          `json:"sessions_abandoned" db:"sessions_abandoned"`
	SessionsFatal         *Bigint          `json:"sessions_fatal" db:"sessions_fatal"`
	SessionsKilled        *Bigint          `json:"sessions_killed" db:"sessions_killed"`
	StatsReset            *TimestampTZ     `json:"stats_reset" db:"stats_reset"`
	ParallelWorkers       *Bigint          `json:"parallel_workers" db:"parallel_workers"`
	TempBytesRead         *Bigint          `json:"temp_bytes_read" db:"temp_bytes_read"`
	TempBytesWritten      *Bigint          `json:"temp_bytes_written" db:"temp_bytes_written"`
	TempFilesRead         *Bigint          `json:"temp_files_read" db:"temp_files_read"`
	TempFilesWritten      *Bigint          `json:"temp_files_written" db:"temp_files_written"`
}

type PgStatDatabasePayload struct {
	Rows []PgStatDatabaseRow `json:"rows"`
}
