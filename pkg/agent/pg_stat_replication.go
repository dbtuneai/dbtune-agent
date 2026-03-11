package agent

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
