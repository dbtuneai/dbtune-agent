package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatProgressCreateIndexRow represents a row from pg_stat_progress_create_index.
type PgStatProgressCreateIndexRow struct {
	PID              *int64  `json:"pid" db:"pid"`
	DatID            *int64  `json:"datid" db:"datid"`
	DatName          *string `json:"datname" db:"datname"`
	RelID            *int64  `json:"relid" db:"relid"`
	IndexRelID       *int64  `json:"index_relid" db:"index_relid"`
	Command          *string `json:"command" db:"command"`
	Phase            *string `json:"phase" db:"phase"`
	LockersTotal     *int64  `json:"lockers_total" db:"lockers_total"`
	LockersDone      *int64  `json:"lockers_done" db:"lockers_done"`
	CurrentLockerPID *int64  `json:"current_locker_pid" db:"current_locker_pid"`
	BlocksTotal      *int64  `json:"blocks_total" db:"blocks_total"`
	BlocksDone       *int64  `json:"blocks_done" db:"blocks_done"`
	TuplesTotal      *int64  `json:"tuples_total" db:"tuples_total"`
	TuplesDone       *int64  `json:"tuples_done" db:"tuples_done"`
	PartitionsTotal  *int64  `json:"partitions_total" db:"partitions_total"`
	PartitionsDone   *int64  `json:"partitions_done" db:"partitions_done"`
}

type PgStatProgressCreateIndexPayload struct {
	Rows []PgStatProgressCreateIndexRow `json:"rows"`
}

const (
	PgStatProgressCreateIndexName     = "pg_stat_progress_create_index"
	PgStatProgressCreateIndexInterval = 30 * time.Second
)

const pgStatProgressCreateIndexQuery = `SELECT * FROM pg_stat_progress_create_index`

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
