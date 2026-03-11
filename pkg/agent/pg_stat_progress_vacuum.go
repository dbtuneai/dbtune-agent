package agent

// PgStatProgressVacuumRow represents a row from pg_stat_progress_vacuum.
type PgStatProgressVacuumRow struct {
	PID              *int64  `json:"pid" db:"pid"`
	DatID            *int64  `json:"datid" db:"datid"`
	DatName          *string `json:"datname" db:"datname"`
	RelID            *int64  `json:"relid" db:"relid"`
	Phase            *string `json:"phase" db:"phase"`
	HeapBlksTotal    *int64  `json:"heap_blks_total" db:"heap_blks_total"`
	HeapBlksScanned  *int64  `json:"heap_blks_scanned" db:"heap_blks_scanned"`
	HeapBlksVacuumed *int64  `json:"heap_blks_vacuumed" db:"heap_blks_vacuumed"`
	IndexVacuumCount *int64  `json:"index_vacuum_count" db:"index_vacuum_count"`
	MaxDeadTuples    *int64  `json:"max_dead_tuples" db:"max_dead_tuples"`
	NumDeadTuples    *int64  `json:"num_dead_tuples" db:"num_dead_tuples"`
}

type PgStatProgressVacuumPayload struct {
	Rows []PgStatProgressVacuumRow `json:"rows"`
}
