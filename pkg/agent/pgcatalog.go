package agent

import "encoding/json"

// PgStatisticRow represents a single row from the pg_stats view,
// matching the backend's PgStatistic Django model.
type PgStatisticRow struct {
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

// PgStatisticPayload is the JSON body POSTed to /api/v1/agent/pg_statistic.
type PgStatisticPayload struct {
	Rows []PgStatisticRow `json:"rows"`
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

// DDLPayload is the JSON body POSTed to /api/v1/agent/ddl.
type DDLPayload struct {
	DDL  string `json:"ddl"`
	Hash string `json:"ddl_hash"`
}
