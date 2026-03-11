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
	DatID                  *int64   `json:"datid" db:"datid"`                                       // pg: oid
	DatName                *string  `json:"datname" db:"datname"`                                   // pg: name
	NumBackends            *int64   `json:"numbackends" db:"numbackends"`                           // pg: integer
	XactCommit             *int64   `json:"xact_commit" db:"xact_commit"`                           // pg: bigint
	XactRollback           *int64   `json:"xact_rollback" db:"xact_rollback"`                       // pg: bigint
	BlksRead               *int64   `json:"blks_read" db:"blks_read"`                               // pg: bigint
	BlksHit                *int64   `json:"blks_hit" db:"blks_hit"`                                 // pg: bigint
	TupReturned            *int64   `json:"tup_returned" db:"tup_returned"`                         // pg: bigint
	TupFetched             *int64   `json:"tup_fetched" db:"tup_fetched"`                           // pg: bigint
	TupInserted            *int64   `json:"tup_inserted" db:"tup_inserted"`                         // pg: bigint
	TupUpdated             *int64   `json:"tup_updated" db:"tup_updated"`                           // pg: bigint
	TupDeleted             *int64   `json:"tup_deleted" db:"tup_deleted"`                           // pg: bigint
	Conflicts              *int64   `json:"conflicts" db:"conflicts"`                               // pg: bigint
	TempFiles              *int64   `json:"temp_files" db:"temp_files"`                             // pg: bigint
	TempBytes              *int64   `json:"temp_bytes" db:"temp_bytes"`                             // pg: bigint
	Deadlocks              *int64   `json:"deadlocks" db:"deadlocks"`                               // pg: bigint
	ChecksumFailures       *int64   `json:"checksum_failures" db:"checksum_failures"`               // pg: bigint
	ChecksumLastFailure    *string  `json:"checksum_last_failure" db:"checksum_last_failure"`       // pg: timestamp with time zone
	BlkReadTime            *float64 `json:"blk_read_time" db:"blk_read_time"`                       // pg: double precision
	BlkWriteTime           *float64 `json:"blk_write_time" db:"blk_write_time"`                     // pg: double precision
	SessionTime            *float64 `json:"session_time" db:"session_time"`                         // pg: double precision
	ActiveTime             *float64 `json:"active_time" db:"active_time"`                           // pg: double precision
	IdleInTransactionTime  *float64 `json:"idle_in_transaction_time" db:"idle_in_transaction_time"` // pg: double precision
	Sessions               *int64   `json:"sessions" db:"sessions"`                                 // pg: bigint
	SessionsAbandoned      *int64   `json:"sessions_abandoned" db:"sessions_abandoned"`             // pg: bigint
	SessionsFatal          *int64   `json:"sessions_fatal" db:"sessions_fatal"`                     // pg: bigint
	SessionsKilled         *int64   `json:"sessions_killed" db:"sessions_killed"`                   // pg: bigint
	StatsReset             *string  `json:"stats_reset" db:"stats_reset"`                           // pg: timestamp with time zone
	ParallelWorkers        *int64   `json:"parallel_workers" db:"parallel_workers"`                 // pg: bigint
	TempBytesRead          *int64   `json:"temp_bytes_read" db:"temp_bytes_read"`                   // pg: bigint
	TempBytesWritten       *int64   `json:"temp_bytes_written" db:"temp_bytes_written"`             // pg: bigint
	TempFilesRead          *int64   `json:"temp_files_read" db:"temp_files_read"`                   // pg: bigint
	TempFilesWritten       *int64   `json:"temp_files_written" db:"temp_files_written"`             // pg: bigint
}

type PgStatDatabasePayload struct {
	Rows []PgStatDatabaseRow `json:"rows"`
}
