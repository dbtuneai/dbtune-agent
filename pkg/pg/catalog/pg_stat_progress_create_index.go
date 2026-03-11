package catalog

// https://www.postgresql.org/docs/current/progress-reporting.html#CREATE-INDEX-PROGRESS-REPORTING

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatProgressCreateIndexRow represents a row from pg_stat_progress_create_index.
type PgStatProgressCreateIndexRow struct {
	PID              *int64  `json:"pid" db:"pid"`                           // pg: integer
	DatID            *int64  `json:"datid" db:"datid"`                       // pg: oid
	DatName          *string `json:"datname" db:"datname"`                   // pg: name
	RelID            *int64  `json:"relid" db:"relid"`                       // pg: oid
	IndexRelID       *int64  `json:"index_relid" db:"index_relid"`           // pg: oid
	Command          *string `json:"command" db:"command"`                   // pg: text
	Phase            *string `json:"phase" db:"phase"`                       // pg: text
	LockersTotal     *int64  `json:"lockers_total" db:"lockers_total"`       // pg: bigint
	LockersDone      *int64  `json:"lockers_done" db:"lockers_done"`         // pg: bigint
	CurrentLockerPID *int64  `json:"current_locker_pid" db:"current_locker_pid"` // pg: bigint
	BlocksTotal      *int64  `json:"blocks_total" db:"blocks_total"`         // pg: bigint
	BlocksDone       *int64  `json:"blocks_done" db:"blocks_done"`           // pg: bigint
	TuplesTotal      *int64  `json:"tuples_total" db:"tuples_total"`         // pg: bigint
	TuplesDone       *int64  `json:"tuples_done" db:"tuples_done"`           // pg: bigint
	PartitionsTotal  *int64  `json:"partitions_total" db:"partitions_total"` // pg: bigint
	PartitionsDone   *int64  `json:"partitions_done" db:"partitions_done"`   // pg: bigint
}

type PgStatProgressCreateIndexPayload struct {
	Rows []PgStatProgressCreateIndexRow `json:"rows"`
}

const (
	PgStatProgressCreateIndexName     = "pg_stat_progress_create_index"
	PgStatProgressCreateIndexInterval = 30 * time.Second
)

const pgStatProgressCreateIndexQuery = `SELECT * FROM pg_stat_progress_create_index WHERE datname = current_database()`

func CollectPgStatProgressCreateIndex(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatProgressCreateIndexRow, error) {
	return CollectView[PgStatProgressCreateIndexRow](pgPool, ctx, pgStatProgressCreateIndexQuery, "pg_stat_progress_create_index")
}

func NewPgStatProgressCreateIndexCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatProgressCreateIndexName,
		Interval: PgStatProgressCreateIndexInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatProgressCreateIndex(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatProgressCreateIndexPayload{Rows: rows}, nil
		},
	}
}
