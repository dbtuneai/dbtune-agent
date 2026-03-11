package agent

// PgStatBgwriterRow represents a row from pg_stat_bgwriter.
type PgStatBgwriterRow struct {
	CheckpointsTimed    *int64   `json:"checkpoints_timed" db:"checkpoints_timed"`
	CheckpointsReq      *int64   `json:"checkpoints_req" db:"checkpoints_req"`
	CheckpointWriteTime *float64 `json:"checkpoint_write_time" db:"checkpoint_write_time"`
	CheckpointSyncTime  *float64 `json:"checkpoint_sync_time" db:"checkpoint_sync_time"`
	BuffersCheckpoint   *int64   `json:"buffers_checkpoint" db:"buffers_checkpoint"`
	BuffersClean        *int64   `json:"buffers_clean" db:"buffers_clean"`
	MaxwrittenClean     *int64   `json:"maxwritten_clean" db:"maxwritten_clean"`
	BuffersBackend      *int64   `json:"buffers_backend" db:"buffers_backend"`
	BuffersBackendFsync *int64   `json:"buffers_backend_fsync" db:"buffers_backend_fsync"`
	BuffersAlloc        *int64   `json:"buffers_alloc" db:"buffers_alloc"`
	StatsReset          *string  `json:"stats_reset" db:"stats_reset"`
}

type PgStatBgwriterPayload struct {
	Rows []PgStatBgwriterRow `json:"rows"`
}
