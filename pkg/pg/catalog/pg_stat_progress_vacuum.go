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
	PID              *int64  `json:"pid" db:"pid"`                           // pg: integer
	DatID            *int64  `json:"datid" db:"datid"`                       // pg: oid
	DatName          *string `json:"datname" db:"datname"`                   // pg: name
	RelID            *int64  `json:"relid" db:"relid"`                       // pg: oid
	Phase            *string `json:"phase" db:"phase"`                       // pg: text
	HeapBlksTotal    *int64  `json:"heap_blks_total" db:"heap_blks_total"`   // pg: bigint
	HeapBlksScanned  *int64  `json:"heap_blks_scanned" db:"heap_blks_scanned"` // pg: bigint
	HeapBlksVacuumed *int64  `json:"heap_blks_vacuumed" db:"heap_blks_vacuumed"` // pg: bigint
	IndexVacuumCount *int64  `json:"index_vacuum_count" db:"index_vacuum_count"` // pg: bigint
	MaxDeadTuples    *int64  `json:"max_dead_tuples" db:"max_dead_tuples"`   // pg: bigint
	NumDeadTuples    *int64  `json:"num_dead_tuples" db:"num_dead_tuples"`   // pg: bigint
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
