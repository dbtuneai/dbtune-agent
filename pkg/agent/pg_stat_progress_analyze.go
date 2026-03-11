package agent

// PgStatProgressAnalyzeRow represents a row from pg_stat_progress_analyze (PG 13+).
type PgStatProgressAnalyzeRow struct {
	PID                    *int64  `json:"pid" db:"pid"`
	DatID                  *int64  `json:"datid" db:"datid"`
	DatName                *string `json:"datname" db:"datname"`
	RelID                  *int64  `json:"relid" db:"relid"`
	Phase                  *string `json:"phase" db:"phase"`
	SampleBlksTotal        *int64  `json:"sample_blks_total" db:"sample_blks_total"`
	SampleBlksScanned      *int64  `json:"sample_blks_scanned" db:"sample_blks_scanned"`
	ExtStatsTotal          *int64  `json:"ext_stats_total" db:"ext_stats_total"`
	ExtStatsComputed       *int64  `json:"ext_stats_computed" db:"ext_stats_computed"`
	ChildTablesTotal       *int64  `json:"child_tables_total" db:"child_tables_total"`
	ChildTablesDone        *int64  `json:"child_tables_done" db:"child_tables_done"`
	CurrentChildTableRelID *int64  `json:"current_child_table_relid" db:"current_child_table_relid"`
}

type PgStatProgressAnalyzePayload struct {
	Rows []PgStatProgressAnalyzeRow `json:"rows"`
}
