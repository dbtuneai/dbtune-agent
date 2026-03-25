package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatActivityRow represents a row from pg_stat_activity.
type PgStatActivityRow struct {
	DatID           *Oid         `json:"datid" db:"datid"`
	DatName         *Name        `json:"datname" db:"datname"`
	PID             *Integer     `json:"pid" db:"pid"`
	LeaderPID       *Integer     `json:"leader_pid" db:"leader_pid"`
	UseSysID        *Oid         `json:"usesysid" db:"usesysid"`
	UseName         *Name        `json:"usename" db:"usename"`
	ApplicationName *Text        `json:"application_name" db:"application_name"`
	ClientAddr      *Inet        `json:"client_addr" db:"client_addr"`
	ClientHostname  *Text        `json:"client_hostname" db:"client_hostname"`
	ClientPort      *Integer     `json:"client_port" db:"client_port"`
	BackendStart    *TimestampTZ `json:"backend_start" db:"backend_start"`
	XactStart       *TimestampTZ `json:"xact_start" db:"xact_start"`
	QueryStart      *TimestampTZ `json:"query_start" db:"query_start"`
	StateChange     *TimestampTZ `json:"state_change" db:"state_change"`
	WaitEventType   *Text        `json:"wait_event_type" db:"wait_event_type"`
	WaitEvent       *Text        `json:"wait_event" db:"wait_event"`
	State           *Text        `json:"state" db:"state"`
	BackendXID      *Xid         `json:"backend_xid" db:"backend_xid"`
	BackendXmin     *Xid         `json:"backend_xmin" db:"backend_xmin"`
	QueryID         *Text        `json:"query_id" db:"query_id"` // PG 14+: composite queryid_usesysid_datid (matches pg_stat_statements key)
	BackendType     *Text        `json:"backend_type" db:"backend_type"`
}

const (
	PgStatActivityName     = "pg_stat_activity"
	PgStatActivityInterval = 1 * time.Minute
)

// Explicit column list to avoid fetching the potentially large query text.
// query_id is constructed as a composite key (queryid_usesysid_datid) matching
// the format used in pg_stat_statements, enabling correlation without needing
// the raw query text. queryid was added in PG 14, so PG 13 uses NULL.
const pgStatActivityQueryPG13 = `SELECT
	datid, datname, pid, leader_pid, usesysid, usename,
	application_name, client_addr, client_hostname, client_port,
	backend_start, xact_start, query_start, state_change,
	wait_event_type, wait_event, state,
	backend_xid, backend_xmin,
	NULL as query_id,
	backend_type
FROM pg_stat_activity
WHERE datname = current_database()`

const pgStatActivityQueryPG14 = `SELECT
	datid, datname, pid, leader_pid, usesysid, usename,
	application_name, client_addr, client_hostname, client_port,
	backend_start, xact_start, query_start, state_change,
	wait_event_type, wait_event, state,
	backend_xid, backend_xmin,
	format('%s_%s_%s', queryid, usesysid, datid) as query_id,
	backend_type
FROM pg_stat_activity
WHERE datname = current_database()`

func PgStatActivityCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	query := pgStatActivityQueryPG14
	if pgMajorVersion < 14 {
		query = pgStatActivityQueryPG13
	}
	return NewCollector[PgStatActivityRow](pool, prepareCtx, PgStatActivityName, PgStatActivityInterval, query)
}
