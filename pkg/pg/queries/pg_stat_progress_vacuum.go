package queries

// https://www.postgresql.org/docs/current/progress-reporting.html#VACUUM-PROGRESS-REPORTING

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

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

const (
	PgStatProgressVacuumName     = "pg_stat_progress_vacuum"
	PgStatProgressVacuumInterval = 30 * time.Second
)

const pgStatProgressVacuumQuery = `SELECT * FROM pg_stat_progress_vacuum WHERE datname = current_database()`

// PgStatProgressVacuumRegistration describes the pgstatprogressvacuum collector's configuration schema.
var PgStatProgressVacuumRegistration = collectorconfig.CollectorRegistration{
	Name: PgStatProgressVacuumName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgStatProgressVacuumCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgStatProgressVacuumRow](pool, prepareCtx, PgStatProgressVacuumName, PgStatProgressVacuumInterval, pgStatProgressVacuumQuery, WithSkipUnchanged())
}
