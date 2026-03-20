package queries

// https://www.postgresql.org/docs/current/progress-reporting.html#VACUUM-PROGRESS-REPORTING

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatProgressVacuumRow represents a row from pg_stat_progress_vacuum.
type PgStatProgressVacuumRow struct {
	PID              *Integer `json:"pid" db:"pid"`
	DatID            *Oid     `json:"datid" db:"datid"`
	DatName          *Name    `json:"datname" db:"datname"`
	RelID            *Oid     `json:"relid" db:"relid"`
	Phase            *Text    `json:"phase" db:"phase"`
	HeapBlksTotal    *Bigint  `json:"heap_blks_total" db:"heap_blks_total"`
	HeapBlksScanned  *Bigint  `json:"heap_blks_scanned" db:"heap_blks_scanned"`
	HeapBlksVacuumed *Bigint  `json:"heap_blks_vacuumed" db:"heap_blks_vacuumed"`
	IndexVacuumCount *Bigint  `json:"index_vacuum_count" db:"index_vacuum_count"`
	MaxDeadTuples    *Bigint  `json:"max_dead_tuples" db:"max_dead_tuples"`
	NumDeadTuples    *Bigint  `json:"num_dead_tuples" db:"num_dead_tuples"`
}

type PgStatProgressVacuumPayload struct {
	Rows []PgStatProgressVacuumRow `json:"rows"`
}

const (
	PgStatProgressVacuumName     = "pg_stat_progress_vacuum"
	PgStatProgressVacuumInterval = 30 * time.Second
)

const pgStatProgressVacuumQuery = `SELECT * FROM pg_stat_progress_vacuum WHERE datname = current_database()`

func CollectPgStatProgressVacuum(pgPool *pgxpool.Pool, ctx context.Context, scanner *pgxutil.Scanner[PgStatProgressVacuumRow]) ([]PgStatProgressVacuumRow, error) {
	return CollectView(pgPool, ctx, pgStatProgressVacuumQuery, "pg_stat_progress_vacuum", scanner)
}

func PgStatProgressVacuumCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	scanner := pgxutil.NewScanner[PgStatProgressVacuumRow]()
	return CatalogCollector{
		Name:     PgStatProgressVacuumName,
		Interval: PgStatProgressVacuumInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatProgressVacuum(pool, ctx, scanner)
			if err != nil {
				return nil, err
			}
			return &PgStatProgressVacuumPayload{Rows: rows}, nil
		},
	}
}
