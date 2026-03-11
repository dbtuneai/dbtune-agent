package agent

// PgStatIORow represents a row from pg_stat_io (PG 16+).
type PgStatIORow struct {
	BackendType   *string  `json:"backend_type" db:"backend_type"`
	Object        *string  `json:"object" db:"object"`
	Context       *string  `json:"context" db:"context"`
	Reads         *int64   `json:"reads" db:"reads"`
	ReadTime      *float64 `json:"read_time" db:"read_time"`
	Writes        *int64   `json:"writes" db:"writes"`
	WriteTime     *float64 `json:"write_time" db:"write_time"`
	Writebacks    *int64   `json:"writebacks" db:"writebacks"`
	WritebackTime *float64 `json:"writeback_time" db:"writeback_time"`
	Extends       *int64   `json:"extends" db:"extends"`
	ExtendTime    *float64 `json:"extend_time" db:"extend_time"`
	OpBytes       *int64   `json:"op_bytes" db:"op_bytes"`
	Hits          *int64   `json:"hits" db:"hits"`
	Evictions     *int64   `json:"evictions" db:"evictions"`
	Reuses        *int64   `json:"reuses" db:"reuses"`
	Fsyncs        *int64   `json:"fsyncs" db:"fsyncs"`
	FsyncTime     *float64 `json:"fsync_time" db:"fsync_time"`
	StatsReset    *string  `json:"stats_reset" db:"stats_reset"`
}

type PgStatIOPayload struct {
	Rows []PgStatIORow `json:"rows"`
}
