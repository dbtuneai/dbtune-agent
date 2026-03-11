package agent

// PgStatDatabaseConflictsRow represents a row from pg_stat_database_conflicts.
type PgStatDatabaseConflictsRow struct {
	DatID           *int64  `json:"datid" db:"datid"`
	DatName         *string `json:"datname" db:"datname"`
	ConflTablespace *int64  `json:"confl_tablespace" db:"confl_tablespace"`
	ConflLock       *int64  `json:"confl_lock" db:"confl_lock"`
	ConflSnapshot   *int64  `json:"confl_snapshot" db:"confl_snapshot"`
	ConflBufferpin  *int64  `json:"confl_bufferpin" db:"confl_bufferpin"`
	ConflDeadlock   *int64  `json:"confl_deadlock" db:"confl_deadlock"`
	ConflLogicalSlot *int64 `json:"confl_active_logicalslot" db:"confl_active_logicalslot"`
}

type PgStatDatabaseConflictsPayload struct {
	Rows []PgStatDatabaseConflictsRow `json:"rows"`
}
