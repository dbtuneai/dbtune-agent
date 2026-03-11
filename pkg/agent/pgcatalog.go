package agent

import "encoding/json"

// PgStatsRow represents a single row from the pg_stats view,
// matching the backend's PgStats Django model.
type PgStatsRow struct {
	SchemaName          string          `json:"schemaname"`
	TableName           string          `json:"tablename"`
	AttName             string          `json:"attname"`
	Inherited           bool            `json:"inherited"`
	NullFrac            *float64        `json:"null_frac"`
	AvgWidth            *int            `json:"avg_width"`
	NDistinct           *float64        `json:"n_distinct"`
	MostCommonVals      json.RawMessage `json:"most_common_vals"`
	MostCommonFreqs     json.RawMessage `json:"most_common_freqs"`
	HistogramBounds     json.RawMessage `json:"histogram_bounds"`
	Correlation         *float64        `json:"correlation"`
	MostCommonElems     json.RawMessage `json:"most_common_elems"`
	MostCommonElemFreqs json.RawMessage `json:"most_common_elem_freqs"`
	ElemCountHistogram  json.RawMessage `json:"elem_count_histogram"`
}

// PgStatsPayload is the JSON body POSTed to /api/v1/agent/pg_stats.
type PgStatsPayload struct {
	Rows []PgStatsRow `json:"rows"`
}

// PgStatUserTableRow represents a single row from pg_stat_user_tables,
// matching the backend's PgStatUserTable Django model.
type PgStatUserTableRow struct {
	RelID                *int64   `json:"relid" db:"relid"`
	SchemaName           string   `json:"schemaname" db:"schemaname"`
	RelName              string   `json:"relname" db:"relname"`
	SeqScan              *int64   `json:"seq_scan" db:"seq_scan"`
	LastSeqScan          *string  `json:"last_seq_scan" db:"last_seq_scan"`
	SeqTupRead           *int64   `json:"seq_tup_read" db:"seq_tup_read"`
	IdxScan              *int64   `json:"idx_scan" db:"idx_scan"`
	LastIdxScan          *string  `json:"last_idx_scan" db:"last_idx_scan"`
	IdxTupFetch          *int64   `json:"idx_tup_fetch" db:"idx_tup_fetch"`
	NTupIns              *int64   `json:"n_tup_ins" db:"n_tup_ins"`
	NTupUpd              *int64   `json:"n_tup_upd" db:"n_tup_upd"`
	NTupDel              *int64   `json:"n_tup_del" db:"n_tup_del"`
	NTupHotUpd           *int64   `json:"n_tup_hot_upd" db:"n_tup_hot_upd"`
	NTupNewpageUpd       *int64   `json:"n_tup_newpage_upd" db:"n_tup_newpage_upd"`
	NLiveTup             *int64   `json:"n_live_tup" db:"n_live_tup"`
	NDeadTup             *int64   `json:"n_dead_tup" db:"n_dead_tup"`
	NModSinceAnalyze     *int64   `json:"n_mod_since_analyze" db:"n_mod_since_analyze"`
	NInsSinceVacuum      *int64   `json:"n_ins_since_vacuum" db:"n_ins_since_vacuum"`
	LastVacuum           *string  `json:"last_vacuum" db:"last_vacuum"`
	LastAutovacuum       *string  `json:"last_autovacuum" db:"last_autovacuum"`
	LastAnalyze          *string  `json:"last_analyze" db:"last_analyze"`
	LastAutoanalyze      *string  `json:"last_autoanalyze" db:"last_autoanalyze"`
	VacuumCount          *int64   `json:"vacuum_count" db:"vacuum_count"`
	AutovacuumCount      *int64   `json:"autovacuum_count" db:"autovacuum_count"`
	AnalyzeCount         *int64   `json:"analyze_count" db:"analyze_count"`
	AutoanalyzeCount     *int64   `json:"autoanalyze_count" db:"autoanalyze_count"`
	TotalVacuumTime      *float64 `json:"total_vacuum_time" db:"total_vacuum_time"`
	TotalAutovacuumTime  *float64 `json:"total_autovacuum_time" db:"total_autovacuum_time"`
	TotalAnalyzeTime     *float64 `json:"total_analyze_time" db:"total_analyze_time"`
	TotalAutoanalyzeTime *float64 `json:"total_autoanalyze_time" db:"total_autoanalyze_time"`
}

// PgStatUserTablePayload is the JSON body POSTed to /api/v1/agent/pg_stat_user_tables.
type PgStatUserTablePayload struct {
	Rows []PgStatUserTableRow `json:"rows"`
}

// PgClassRow represents a single row from pg_class for user tables,
// providing reltuples and relpages needed for index recommendation.
type PgClassRow struct {
	SchemaName string  `json:"schemaname"`
	RelName    string  `json:"relname"`
	RelTuples  float64 `json:"reltuples"`
	RelPages   int     `json:"relpages"`
}

// PgClassPayload is the JSON body POSTed to /api/v1/agent/pg_class.
type PgClassPayload struct {
	Rows []PgClassRow `json:"rows"`
}

// PgStatActivityRow represents a row from pg_stat_activity.
type PgStatActivityRow struct {
	DatID           *int64  `json:"datid" db:"datid"`
	DatName         *string `json:"datname" db:"datname"`
	PID             *int64  `json:"pid" db:"pid"`
	LeaderPID       *int64  `json:"leader_pid" db:"leader_pid"`
	UseSysID        *int64  `json:"usesysid" db:"usesysid"`
	UseName         *string `json:"usename" db:"usename"`
	ApplicationName *string `json:"application_name" db:"application_name"`
	ClientAddr      *string `json:"client_addr" db:"client_addr"`
	ClientHostname  *string `json:"client_hostname" db:"client_hostname"`
	ClientPort      *int64  `json:"client_port" db:"client_port"`
	BackendStart    *string `json:"backend_start" db:"backend_start"`
	XactStart       *string `json:"xact_start" db:"xact_start"`
	QueryStart      *string `json:"query_start" db:"query_start"`
	StateChange     *string `json:"state_change" db:"state_change"`
	WaitEventType   *string `json:"wait_event_type" db:"wait_event_type"`
	WaitEvent       *string `json:"wait_event" db:"wait_event"`
	State           *string `json:"state" db:"state"`
	BackendXID      *string `json:"backend_xid" db:"backend_xid"`
	BackendXmin     *string `json:"backend_xmin" db:"backend_xmin"`
	QueryID         *int64  `json:"query_id" db:"query_id"`
	Query           *string `json:"query" db:"query"`
	BackendType     *string `json:"backend_type" db:"backend_type"`
}

type PgStatActivityPayload struct {
	Rows []PgStatActivityRow `json:"rows"`
}

// PgStatDatabaseRow represents a row from pg_stat_database.
type PgStatDatabaseRow struct {
	DatID                  *int64   `json:"datid" db:"datid"`
	DatName                *string  `json:"datname" db:"datname"`
	NumBackends            *int64   `json:"numbackends" db:"numbackends"`
	XactCommit             *int64   `json:"xact_commit" db:"xact_commit"`
	XactRollback           *int64   `json:"xact_rollback" db:"xact_rollback"`
	BlksRead               *int64   `json:"blks_read" db:"blks_read"`
	BlksHit                *int64   `json:"blks_hit" db:"blks_hit"`
	TupReturned            *int64   `json:"tup_returned" db:"tup_returned"`
	TupFetched             *int64   `json:"tup_fetched" db:"tup_fetched"`
	TupInserted            *int64   `json:"tup_inserted" db:"tup_inserted"`
	TupUpdated             *int64   `json:"tup_updated" db:"tup_updated"`
	TupDeleted             *int64   `json:"tup_deleted" db:"tup_deleted"`
	Conflicts              *int64   `json:"conflicts" db:"conflicts"`
	TempFiles              *int64   `json:"temp_files" db:"temp_files"`
	TempBytes              *int64   `json:"temp_bytes" db:"temp_bytes"`
	Deadlocks              *int64   `json:"deadlocks" db:"deadlocks"`
	ChecksumFailures       *int64   `json:"checksum_failures" db:"checksum_failures"`
	ChecksumLastFailure    *string  `json:"checksum_last_failure" db:"checksum_last_failure"`
	BlkReadTime            *float64 `json:"blk_read_time" db:"blk_read_time"`
	BlkWriteTime           *float64 `json:"blk_write_time" db:"blk_write_time"`
	SessionTime            *float64 `json:"session_time" db:"session_time"`
	ActiveTime             *float64 `json:"active_time" db:"active_time"`
	IdleInTransactionTime  *float64 `json:"idle_in_transaction_time" db:"idle_in_transaction_time"`
	Sessions               *int64   `json:"sessions" db:"sessions"`
	SessionsAbandoned      *int64   `json:"sessions_abandoned" db:"sessions_abandoned"`
	SessionsFatal          *int64   `json:"sessions_fatal" db:"sessions_fatal"`
	SessionsKilled         *int64   `json:"sessions_killed" db:"sessions_killed"`
	StatsReset             *string  `json:"stats_reset" db:"stats_reset"`
	ParallelWorkers        *int64   `json:"parallel_workers" db:"parallel_workers"`
	TempBytesRead          *int64   `json:"temp_bytes_read" db:"temp_bytes_read"`
	TempBytesWritten       *int64   `json:"temp_bytes_written" db:"temp_bytes_written"`
	TempFilesRead          *int64   `json:"temp_files_read" db:"temp_files_read"`
	TempFilesWritten       *int64   `json:"temp_files_written" db:"temp_files_written"`
}

type PgStatDatabasePayload struct {
	Rows []PgStatDatabaseRow `json:"rows"`
}

// PgStatDatabaseConflictsRow represents a row from pg_stat_database_conflicts.
type PgStatDatabaseConflictsRow struct {
	DatID          *int64  `json:"datid" db:"datid"`
	DatName        *string `json:"datname" db:"datname"`
	ConflTablespace *int64 `json:"confl_tablespace" db:"confl_tablespace"`
	ConflLock      *int64  `json:"confl_lock" db:"confl_lock"`
	ConflSnapshot  *int64  `json:"confl_snapshot" db:"confl_snapshot"`
	ConflBufferpin *int64  `json:"confl_bufferpin" db:"confl_bufferpin"`
	ConflDeadlock  *int64  `json:"confl_deadlock" db:"confl_deadlock"`
	ConflLogicalSlot *int64 `json:"confl_active_logicalslot" db:"confl_active_logicalslot"`
}

type PgStatDatabaseConflictsPayload struct {
	Rows []PgStatDatabaseConflictsRow `json:"rows"`
}

// PgStatArchiverRow represents a row from pg_stat_archiver.
type PgStatArchiverRow struct {
	ArchivedCount    *int64  `json:"archived_count" db:"archived_count"`
	LastArchivedWal  *string `json:"last_archived_wal" db:"last_archived_wal"`
	LastArchivedTime *string `json:"last_archived_time" db:"last_archived_time"`
	FailedCount      *int64  `json:"failed_count" db:"failed_count"`
	LastFailedWal    *string `json:"last_failed_wal" db:"last_failed_wal"`
	LastFailedTime   *string `json:"last_failed_time" db:"last_failed_time"`
	StatsReset       *string `json:"stats_reset" db:"stats_reset"`
}

type PgStatArchiverPayload struct {
	Rows []PgStatArchiverRow `json:"rows"`
}

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

// PgStatCheckpointerRow represents a row from pg_stat_checkpointer (PG 17+).
type PgStatCheckpointerRow struct {
	NumTimed          *int64   `json:"num_timed" db:"num_timed"`
	NumRequested      *int64   `json:"num_requested" db:"num_requested"`
	RestartpointsTimed    *int64 `json:"restartpoints_timed" db:"restartpoints_timed"`
	RestartpointsReq      *int64 `json:"restartpoints_req" db:"restartpoints_req"`
	RestartpointsDone     *int64 `json:"restartpoints_done" db:"restartpoints_done"`
	WriteTime         *float64 `json:"write_time" db:"write_time"`
	SyncTime          *float64 `json:"sync_time" db:"sync_time"`
	BuffersWritten    *int64   `json:"buffers_written" db:"buffers_written"`
	StatsReset        *string  `json:"stats_reset" db:"stats_reset"`
	SlruWritten       *int64   `json:"slru_written" db:"slru_written"`
}

type PgStatCheckpointerPayload struct {
	Rows []PgStatCheckpointerRow `json:"rows"`
}

// PgStatWalRow represents a row from pg_stat_wal (PG 14+).
type PgStatWalRow struct {
	WalRecords     *int64   `json:"wal_records" db:"wal_records"`
	WalFpi         *int64   `json:"wal_fpi" db:"wal_fpi"`
	WalBytes       *int64   `json:"wal_bytes" db:"wal_bytes"`
	WalBuffersFull *int64   `json:"wal_buffers_full" db:"wal_buffers_full"`
	WalWrite       *int64   `json:"wal_write" db:"wal_write"`
	WalSync        *int64   `json:"wal_sync" db:"wal_sync"`
	WalWriteTime   *float64 `json:"wal_write_time" db:"wal_write_time"`
	WalSyncTime    *float64 `json:"wal_sync_time" db:"wal_sync_time"`
	StatsReset     *string  `json:"stats_reset" db:"stats_reset"`
}

type PgStatWalPayload struct {
	Rows []PgStatWalRow `json:"rows"`
}

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

// PgStatReplicationRow represents a row from pg_stat_replication.
type PgStatReplicationRow struct {
	PID             *int64  `json:"pid" db:"pid"`
	UseSysID        *int64  `json:"usesysid" db:"usesysid"`
	UseName         *string `json:"usename" db:"usename"`
	ApplicationName *string `json:"application_name" db:"application_name"`
	ClientAddr      *string `json:"client_addr" db:"client_addr"`
	ClientHostname  *string `json:"client_hostname" db:"client_hostname"`
	ClientPort      *int64  `json:"client_port" db:"client_port"`
	BackendStart    *string `json:"backend_start" db:"backend_start"`
	BackendXmin     *string `json:"backend_xmin" db:"backend_xmin"`
	State           *string `json:"state" db:"state"`
	SentLsn         *string `json:"sent_lsn" db:"sent_lsn"`
	WriteLsn        *string `json:"write_lsn" db:"write_lsn"`
	FlushLsn        *string `json:"flush_lsn" db:"flush_lsn"`
	ReplayLsn       *string `json:"replay_lsn" db:"replay_lsn"`
	WriteLag        *string `json:"write_lag" db:"write_lag"`
	FlushLag        *string `json:"flush_lag" db:"flush_lag"`
	ReplayLag       *string `json:"replay_lag" db:"replay_lag"`
	SyncPriority    *int64  `json:"sync_priority" db:"sync_priority"`
	SyncState       *string `json:"sync_state" db:"sync_state"`
	ReplyTime       *string `json:"reply_time" db:"reply_time"`
}

type PgStatReplicationPayload struct {
	Rows []PgStatReplicationRow `json:"rows"`
}

// PgStatReplicationSlotsRow represents a row from pg_stat_replication_slots (PG 14+).
type PgStatReplicationSlotsRow struct {
	SlotName          *string `json:"slot_name" db:"slot_name"`
	SpillTxns         *int64  `json:"spill_txns" db:"spill_txns"`
	SpillCount        *int64  `json:"spill_count" db:"spill_count"`
	SpillBytes        *int64  `json:"spill_bytes" db:"spill_bytes"`
	StreamTxns        *int64  `json:"stream_txns" db:"stream_txns"`
	StreamCount       *int64  `json:"stream_count" db:"stream_count"`
	StreamBytes       *int64  `json:"stream_bytes" db:"stream_bytes"`
	TotalTxns         *int64  `json:"total_txns" db:"total_txns"`
	TotalBytes        *int64  `json:"total_bytes" db:"total_bytes"`
	StatsReset        *string `json:"stats_reset" db:"stats_reset"`
}

type PgStatReplicationSlotsPayload struct {
	Rows []PgStatReplicationSlotsRow `json:"rows"`
}

// PgStatSlruRow represents a row from pg_stat_slru.
type PgStatSlruRow struct {
	Name        *string `json:"name" db:"name"`
	BlksZeroed  *int64  `json:"blks_zeroed" db:"blks_zeroed"`
	BlksHit     *int64  `json:"blks_hit" db:"blks_hit"`
	BlksRead    *int64  `json:"blks_read" db:"blks_read"`
	BlksWritten *int64  `json:"blks_written" db:"blks_written"`
	BlksExists  *int64  `json:"blks_exists" db:"blks_exists"`
	Flushes     *int64  `json:"flushes" db:"flushes"`
	Truncates   *int64  `json:"truncates" db:"truncates"`
	StatsReset  *string `json:"stats_reset" db:"stats_reset"`
}

type PgStatSlruPayload struct {
	Rows []PgStatSlruRow `json:"rows"`
}

// PgStatUserIndexesRow represents a row from pg_stat_user_indexes.
type PgStatUserIndexesRow struct {
	RelID       *int64  `json:"relid" db:"relid"`
	IndexRelID  *int64  `json:"indexrelid" db:"indexrelid"`
	SchemaName  *string `json:"schemaname" db:"schemaname"`
	RelName     *string `json:"relname" db:"relname"`
	IndexRelName *string `json:"indexrelname" db:"indexrelname"`
	IdxScan     *int64  `json:"idx_scan" db:"idx_scan"`
	LastIdxScan *string `json:"last_idx_scan" db:"last_idx_scan"`
	IdxTupRead  *int64  `json:"idx_tup_read" db:"idx_tup_read"`
	IdxTupFetch *int64  `json:"idx_tup_fetch" db:"idx_tup_fetch"`
}

type PgStatUserIndexesPayload struct {
	Rows []PgStatUserIndexesRow `json:"rows"`
}

// PgStatioUserTablesRow represents a row from pg_statio_user_tables.
type PgStatioUserTablesRow struct {
	RelID          *int64  `json:"relid" db:"relid"`
	SchemaName     *string `json:"schemaname" db:"schemaname"`
	RelName        *string `json:"relname" db:"relname"`
	HeapBlksRead   *int64  `json:"heap_blks_read" db:"heap_blks_read"`
	HeapBlksHit    *int64  `json:"heap_blks_hit" db:"heap_blks_hit"`
	IdxBlksRead    *int64  `json:"idx_blks_read" db:"idx_blks_read"`
	IdxBlksHit     *int64  `json:"idx_blks_hit" db:"idx_blks_hit"`
	ToastBlksRead  *int64  `json:"toast_blks_read" db:"toast_blks_read"`
	ToastBlksHit   *int64  `json:"toast_blks_hit" db:"toast_blks_hit"`
	TidxBlksRead   *int64  `json:"tidx_blks_read" db:"tidx_blks_read"`
	TidxBlksHit    *int64  `json:"tidx_blks_hit" db:"tidx_blks_hit"`
}

type PgStatioUserTablesPayload struct {
	Rows []PgStatioUserTablesRow `json:"rows"`
}

// PgStatioUserIndexesRow represents a row from pg_statio_user_indexes.
type PgStatioUserIndexesRow struct {
	RelID        *int64  `json:"relid" db:"relid"`
	IndexRelID   *int64  `json:"indexrelid" db:"indexrelid"`
	SchemaName   *string `json:"schemaname" db:"schemaname"`
	RelName      *string `json:"relname" db:"relname"`
	IndexRelName *string `json:"indexrelname" db:"indexrelname"`
	IdxBlksRead  *int64  `json:"idx_blks_read" db:"idx_blks_read"`
	IdxBlksHit   *int64  `json:"idx_blks_hit" db:"idx_blks_hit"`
}

type PgStatioUserIndexesPayload struct {
	Rows []PgStatioUserIndexesRow `json:"rows"`
}

// PgStatUserFunctionsRow represents a row from pg_stat_user_functions.
type PgStatUserFunctionsRow struct {
	FuncID     *int64   `json:"funcid" db:"funcid"`
	SchemaName *string  `json:"schemaname" db:"schemaname"`
	FuncName   *string  `json:"funcname" db:"funcname"`
	Calls      *int64   `json:"calls" db:"calls"`
	TotalTime  *float64 `json:"total_time" db:"total_time"`
	SelfTime   *float64 `json:"self_time" db:"self_time"`
}

type PgStatUserFunctionsPayload struct {
	Rows []PgStatUserFunctionsRow `json:"rows"`
}

// PgLocksRow represents a filtered row from pg_locks (blocked + blockers only).
type PgLocksRow struct {
	LockType         *string `json:"locktype" db:"locktype"`
	Database         *int64  `json:"database" db:"database"`
	Relation         *int64  `json:"relation" db:"relation"`
	Page             *int64  `json:"page" db:"page"`
	Tuple            *int64  `json:"tuple" db:"tuple"`
	VirtualXID       *string `json:"virtualxid" db:"virtualxid"`
	TransactionID    *string `json:"transactionid" db:"transactionid"`
	ClassID          *int64  `json:"classid" db:"classid"`
	ObjID            *int64  `json:"objid" db:"objid"`
	ObjSubID         *int64  `json:"objsubid" db:"objsubid"`
	VirtualTransaction *string `json:"virtualtransaction" db:"virtualtransaction"`
	PID              *int64  `json:"pid" db:"pid"`
	Mode             *string `json:"mode" db:"mode"`
	Granted          *bool   `json:"granted" db:"granted"`
	FastPath         *bool   `json:"fastpath" db:"fastpath"`
	WaitStart        *string `json:"waitstart" db:"waitstart"`
}

type PgLocksPayload struct {
	Rows []PgLocksRow `json:"rows"`
}

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

// PgStatProgressAnalyzeRow represents a row from pg_stat_progress_analyze (PG 13+).
type PgStatProgressAnalyzeRow struct {
	PID                   *int64  `json:"pid" db:"pid"`
	DatID                 *int64  `json:"datid" db:"datid"`
	DatName               *string `json:"datname" db:"datname"`
	RelID                 *int64  `json:"relid" db:"relid"`
	Phase                 *string `json:"phase" db:"phase"`
	SampleBlksTotal       *int64  `json:"sample_blks_total" db:"sample_blks_total"`
	SampleBlksScanned     *int64  `json:"sample_blks_scanned" db:"sample_blks_scanned"`
	ExtStatsTotal         *int64  `json:"ext_stats_total" db:"ext_stats_total"`
	ExtStatsComputed      *int64  `json:"ext_stats_computed" db:"ext_stats_computed"`
	ChildTablesTotal      *int64  `json:"child_tables_total" db:"child_tables_total"`
	ChildTablesDone       *int64  `json:"child_tables_done" db:"child_tables_done"`
	CurrentChildTableRelID *int64 `json:"current_child_table_relid" db:"current_child_table_relid"`
}

type PgStatProgressAnalyzePayload struct {
	Rows []PgStatProgressAnalyzeRow `json:"rows"`
}

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

// PgPreparedXactsRow represents a row from pg_prepared_xacts.
type PgPreparedXactsRow struct {
	Transaction *string `json:"transaction" db:"transaction"`
	GID         *string `json:"gid" db:"gid"`
	Prepared    *string `json:"prepared" db:"prepared"`
	Owner       *string `json:"owner" db:"owner"`
	Database    *string `json:"database" db:"database"`
}

type PgPreparedXactsPayload struct {
	Rows []PgPreparedXactsRow `json:"rows"`
}

// PgReplicationSlotsRow represents a row from pg_replication_slots.
type PgReplicationSlotsRow struct {
	SlotName           *string `json:"slot_name" db:"slot_name"`
	Plugin             *string `json:"plugin" db:"plugin"`
	SlotType           *string `json:"slot_type" db:"slot_type"`
	DatOID             *int64  `json:"datoid" db:"datoid"`
	Database           *string `json:"database" db:"database"`
	Temporary          *bool   `json:"temporary" db:"temporary"`
	Active             *bool   `json:"active" db:"active"`
	ActivePID          *int64  `json:"active_pid" db:"active_pid"`
	Xmin               *string `json:"xmin" db:"xmin"`
	CatalogXmin        *string `json:"catalog_xmin" db:"catalog_xmin"`
	RestartLsn         *string `json:"restart_lsn" db:"restart_lsn"`
	ConfirmedFlushLsn  *string `json:"confirmed_flush_lsn" db:"confirmed_flush_lsn"`
	WalStatus          *string `json:"wal_status" db:"wal_status"`
	SafeWalSize        *int64  `json:"safe_wal_size" db:"safe_wal_size"`
	TwoPhase           *string `json:"two_phase" db:"two_phase"`
	Conflicting        *string `json:"conflicting" db:"conflicting"`
	InvalidationReason *string `json:"invalidation_reason" db:"invalidation_reason"`
}

type PgReplicationSlotsPayload struct {
	Rows []PgReplicationSlotsRow `json:"rows"`
}

// PgIndexRow represents a row from pg_index joined with pg_class/pg_namespace.
type PgIndexRow struct {
	SchemaName      *string  `json:"schemaname" db:"schemaname"`
	TableName       *string  `json:"tablename" db:"tablename"`
	IndexName       *string  `json:"indexname" db:"indexname"`
	IndexRelID      *int64   `json:"indexrelid" db:"indexrelid"`
	IndRelID        *int64   `json:"indrelid" db:"indrelid"`
	IndNatts        *int64   `json:"indnatts" db:"indnatts"`
	IndNKeyAtts     *int64   `json:"indnkeyatts" db:"indnkeyatts"`
	IndIsUnique     *bool    `json:"indisunique" db:"indisunique"`
	IndIsPrimary    *bool    `json:"indisprimary" db:"indisprimary"`
	IndIsExclusion  *bool    `json:"indisexclusion" db:"indisexclusion"`
	IndImmediate    *bool    `json:"indimmediate" db:"indimmediate"`
	IndIsClustered  *bool    `json:"indisclustered" db:"indisclustered"`
	IndIsValid      *bool    `json:"indisvalid" db:"indisvalid"`
	IndCheckXmin    *bool    `json:"indcheckxmin" db:"indcheckxmin"`
	IndIsReady      *bool    `json:"indisready" db:"indisready"`
	IndIsLive       *bool    `json:"indislive" db:"indislive"`
	IndIsReplIdent  *bool    `json:"indisreplident" db:"indisreplident"`
	RelTuples       *float64 `json:"reltuples" db:"reltuples"`
	IndexDef        *string  `json:"indexdef" db:"indexdef"`
}

type PgIndexPayload struct {
	Rows []PgIndexRow `json:"rows"`
}

// PgStatWalReceiverRow represents a row from pg_stat_wal_receiver (no conninfo).
type PgStatWalReceiverRow struct {
	PID                *int64  `json:"pid" db:"pid"`
	Status             *string `json:"status" db:"status"`
	ReceiveStartLsn    *string `json:"receive_start_lsn" db:"receive_start_lsn"`
	ReceiveStartTli    *int64  `json:"receive_start_tli" db:"receive_start_tli"`
	WrittenLsn         *string `json:"written_lsn" db:"written_lsn"`
	FlushedLsn         *string `json:"flushed_lsn" db:"flushed_lsn"`
	ReceivedTli        *int64  `json:"received_tli" db:"received_tli"`
	LastMsgSendTime    *string `json:"last_msg_send_time" db:"last_msg_send_time"`
	LastMsgReceiptTime *string `json:"last_msg_receipt_time" db:"last_msg_receipt_time"`
	LatestEndLsn       *string `json:"latest_end_lsn" db:"latest_end_lsn"`
	LatestEndTime      *string `json:"latest_end_time" db:"latest_end_time"`
	SlotName           *string `json:"slot_name" db:"slot_name"`
	SenderHost         *string `json:"sender_host" db:"sender_host"`
	SenderPort         *int64  `json:"sender_port" db:"sender_port"`
}

type PgStatWalReceiverPayload struct {
	Rows []PgStatWalReceiverRow `json:"rows"`
}

// PgStatRecoveryPrefetchRow represents a row from pg_stat_recovery_prefetch (PG 15+).
type PgStatRecoveryPrefetchRow struct {
	StatsReset    *string `json:"stats_reset" db:"stats_reset"`
	Prefetch      *int64  `json:"prefetch" db:"prefetch"`
	Hit           *int64  `json:"hit" db:"hit"`
	SkipInit      *int64  `json:"skip_init" db:"skip_init"`
	SkipNew       *int64  `json:"skip_new" db:"skip_new"`
	SkipFpw       *int64  `json:"skip_fpw" db:"skip_fpw"`
	SkipRep       *int64  `json:"skip_rep" db:"skip_rep"`
	WalDistance   *int64  `json:"wal_distance" db:"wal_distance"`
	BlockDistance  *int64  `json:"block_distance" db:"block_distance"`
	IoDepth       *int64  `json:"io_depth" db:"io_depth"`
}

type PgStatRecoveryPrefetchPayload struct {
	Rows []PgStatRecoveryPrefetchRow `json:"rows"`
}

// PgStatSubscriptionRow represents a row from pg_stat_subscription.
type PgStatSubscriptionRow struct {
	SubID              *int64  `json:"subid" db:"subid"`
	SubName            *string `json:"subname" db:"subname"`
	PID                *int64  `json:"pid" db:"pid"`
	LeaderPID          *int64  `json:"leader_pid" db:"leader_pid"`
	RelID              *int64  `json:"relid" db:"relid"`
	ReceivedLsn        *string `json:"received_lsn" db:"received_lsn"`
	LastMsgSendTime    *string `json:"last_msg_send_time" db:"last_msg_send_time"`
	LastMsgReceiptTime *string `json:"last_msg_receipt_time" db:"last_msg_receipt_time"`
	LatestEndLsn       *string `json:"latest_end_lsn" db:"latest_end_lsn"`
	LatestEndTime      *string `json:"latest_end_time" db:"latest_end_time"`
	WorkerType         *string `json:"worker_type" db:"worker_type"`
}

type PgStatSubscriptionPayload struct {
	Rows []PgStatSubscriptionRow `json:"rows"`
}

// PgStatSubscriptionStatsRow represents a row from pg_stat_subscription_stats (PG 15+).
type PgStatSubscriptionStatsRow struct {
	SubID           *int64  `json:"subid" db:"subid"`
	SubName         *string `json:"subname" db:"subname"`
	ApplyErrorCount *int64  `json:"apply_error_count" db:"apply_error_count"`
	SyncErrorCount  *int64  `json:"sync_error_count" db:"sync_error_count"`
	StatsReset      *string `json:"stats_reset" db:"stats_reset"`
}

type PgStatSubscriptionStatsPayload struct {
	Rows []PgStatSubscriptionStatsRow `json:"rows"`
}

// DDLPayload is the JSON body POSTed to /api/v1/agent/ddl.
type DDLPayload struct {
	DDL  string `json:"ddl"`
	Hash string `json:"ddl_hash"`
}
