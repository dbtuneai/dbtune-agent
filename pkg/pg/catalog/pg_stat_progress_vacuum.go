package catalog

// https://www.postgresql.org/docs/current/progress-reporting.html#VACUUM-PROGRESS-REPORTING

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatProgressVacuumName     = "pg_stat_progress_vacuum"
	PgStatProgressVacuumInterval = 30 * time.Second
)

const pgStatProgressVacuumQuery = `SELECT * FROM pg_stat_progress_vacuum WHERE datname = current_database()`

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

func CollectPgStatProgressVacuum(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatProgressVacuumRow, error) {
	return CollectView[PgStatProgressVacuumRow](pgPool, ctx, pgStatProgressVacuumQuery, "pg_stat_progress_vacuum")
}

func NewPgStatProgressVacuumCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatProgressVacuumName,
		Interval: PgStatProgressVacuumInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatProgressVacuum(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatProgressVacuumPayload{Rows: rows}, nil
		},
	}
}
