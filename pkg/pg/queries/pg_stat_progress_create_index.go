package queries

// https://www.postgresql.org/docs/current/progress-reporting.html#CREATE-INDEX-PROGRESS-REPORTING

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatProgressCreateIndexRow represents a row from pg_stat_progress_create_index.
type PgStatProgressCreateIndexRow struct {
	PID              *Integer `json:"pid" db:"pid"`
	DatID            *Oid     `json:"datid" db:"datid"`
	DatName          *Name    `json:"datname" db:"datname"`
	RelID            *Oid     `json:"relid" db:"relid"`
	IndexRelID       *Oid     `json:"index_relid" db:"index_relid"`
	Command          *Text    `json:"command" db:"command"`
	Phase            *Text    `json:"phase" db:"phase"`
	LockersTotal     *Bigint  `json:"lockers_total" db:"lockers_total"`
	LockersDone      *Bigint  `json:"lockers_done" db:"lockers_done"`
	CurrentLockerPID *Bigint  `json:"current_locker_pid" db:"current_locker_pid"`
	BlocksTotal      *Bigint  `json:"blocks_total" db:"blocks_total"`
	BlocksDone       *Bigint  `json:"blocks_done" db:"blocks_done"`
	TuplesTotal      *Bigint  `json:"tuples_total" db:"tuples_total"`
	TuplesDone       *Bigint  `json:"tuples_done" db:"tuples_done"`
	PartitionsTotal  *Bigint  `json:"partitions_total" db:"partitions_total"`
	PartitionsDone   *Bigint  `json:"partitions_done" db:"partitions_done"`
}

type PgStatProgressCreateIndexPayload struct {
	Rows []PgStatProgressCreateIndexRow `json:"rows"`
}

const (
	PgStatProgressCreateIndexName     = "pg_stat_progress_create_index"
	PgStatProgressCreateIndexInterval = 30 * time.Second
)

const pgStatProgressCreateIndexQuery = `SELECT * FROM pg_stat_progress_create_index WHERE datname = current_database()`

func CollectPgStatProgressCreateIndex(pgPool *pgxpool.Pool, ctx context.Context, scanner *pgxutil.Scanner[PgStatProgressCreateIndexRow]) ([]PgStatProgressCreateIndexRow, error) {
	return CollectView(pgPool, ctx, pgStatProgressCreateIndexQuery, "pg_stat_progress_create_index", scanner)
}

func PgStatProgressCreateIndexCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	scanner := pgxutil.NewScanner[PgStatProgressCreateIndexRow]()
	return CatalogCollector{
		Name:     PgStatProgressCreateIndexName,
		Interval: PgStatProgressCreateIndexInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatProgressCreateIndex(pool, ctx, scanner)
			if err != nil {
				return nil, err
			}
			return &PgStatProgressCreateIndexPayload{Rows: rows}, nil
		},
	}
}
