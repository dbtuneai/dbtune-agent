package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatActivityName     = "pg_stat_activity"
	PgStatActivityInterval = 1 * time.Minute
)

const pgStatActivityQuery = `SELECT * FROM pg_stat_activity WHERE datname = current_database()`

func CollectPgStatActivity(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatActivityRow, error) {
	return CollectView[PgStatActivityRow](pgPool, ctx, pgStatActivityQuery, "pg_stat_activity")
}

func PgStatActivityCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
		Name:     PgStatActivityName,
		Interval: PgStatActivityInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatActivity(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatActivityPayload{Rows: rows}, nil
		},
	}
}

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
	QueryID         *Bigint      `json:"query_id" db:"query_id"`
	Query           *Text        `json:"query" db:"query"`
	BackendType     *Text        `json:"backend_type" db:"backend_type"`
}

type PgStatActivityPayload struct {
	Rows []PgStatActivityRow `json:"rows"`
}
