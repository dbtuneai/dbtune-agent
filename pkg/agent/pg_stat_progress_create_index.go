package agent

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
