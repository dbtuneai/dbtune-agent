package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
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

func NewPgStatDatabaseCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
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
	DatID                  *int64   `json:"datid" db:"datid"`
	DatName                *string  `json:"datname" db:"datname"`
	NumBackends            *int64   `json:"numbackends" db:"numbackends"`
	XactCommit             *int64   `json:"xact_commit" db:"xact_commit"`
	XactRollback           *int64   `json:"xact_rollback" db:"xact_rollback"`
	BlksRead               *int64   `json:"blks_read" db:"blks_read"`
	BlksHit                *int64   `json:"blks_hit" db:"blks_hit"`
	TupReturned            *int64   `json:"tup_returned" db:"tup_returned"`
	TupFetched             *int64   `json:"tup_fetched" db:"tup_fetched"`
	TupInserted            *int64   `json:"tup_inserted" db:"tup_inserted"`
	TupUpdated             *int64   `json:"tup_updated" db:"tup_updated"`
	TupDeleted             *int64   `json:"tup_deleted" db:"tup_deleted"`
	Conflicts              *int64   `json:"conflicts" db:"conflicts"`
	TempFiles              *int64   `json:"temp_files" db:"temp_files"`
	TempBytes              *int64   `json:"temp_bytes" db:"temp_bytes"`
	Deadlocks              *int64   `json:"deadlocks" db:"deadlocks"`
	ChecksumFailures       *int64   `json:"checksum_failures" db:"checksum_failures"`
	ChecksumLastFailure    *string  `json:"checksum_last_failure" db:"checksum_last_failure"`
	BlkReadTime            *float64 `json:"blk_read_time" db:"blk_read_time"`
	BlkWriteTime           *float64 `json:"blk_write_time" db:"blk_write_time"`
	SessionTime            *float64 `json:"session_time" db:"session_time"`
	ActiveTime             *float64 `json:"active_time" db:"active_time"`
	IdleInTransactionTime  *float64 `json:"idle_in_transaction_time" db:"idle_in_transaction_time"`
	Sessions               *int64   `json:"sessions" db:"sessions"`
	SessionsAbandoned      *int64   `json:"sessions_abandoned" db:"sessions_abandoned"`
	SessionsFatal          *int64   `json:"sessions_fatal" db:"sessions_fatal"`
	SessionsKilled         *int64   `json:"sessions_killed" db:"sessions_killed"`
	StatsReset             *string  `json:"stats_reset" db:"stats_reset"`
	ParallelWorkers        *int64   `json:"parallel_workers" db:"parallel_workers"`
	TempBytesRead          *int64   `json:"temp_bytes_read" db:"temp_bytes_read"`
	TempBytesWritten       *int64   `json:"temp_bytes_written" db:"temp_bytes_written"`
	TempFilesRead          *int64   `json:"temp_files_read" db:"temp_files_read"`
	TempFilesWritten       *int64   `json:"temp_files_written" db:"temp_files_written"`
}

type PgStatDatabasePayload struct {
	Rows []PgStatDatabaseRow `json:"rows"`
}
