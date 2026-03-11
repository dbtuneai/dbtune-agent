package agent

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
