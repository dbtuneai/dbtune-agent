package agent

// PgStatCheckpointerRow represents a row from pg_stat_checkpointer (PG 17+).
type PgStatCheckpointerRow struct {
	NumTimed           *int64   `json:"num_timed" db:"num_timed"`
	NumRequested       *int64   `json:"num_requested" db:"num_requested"`
	RestartpointsTimed *int64   `json:"restartpoints_timed" db:"restartpoints_timed"`
	RestartpointsReq   *int64   `json:"restartpoints_req" db:"restartpoints_req"`
	RestartpointsDone  *int64   `json:"restartpoints_done" db:"restartpoints_done"`
	WriteTime          *float64 `json:"write_time" db:"write_time"`
	SyncTime           *float64 `json:"sync_time" db:"sync_time"`
	BuffersWritten     *int64   `json:"buffers_written" db:"buffers_written"`
	StatsReset         *string  `json:"stats_reset" db:"stats_reset"`
	SlruWritten        *int64   `json:"slru_written" db:"slru_written"`
}

type PgStatCheckpointerPayload struct {
	Rows []PgStatCheckpointerRow `json:"rows"`
}
