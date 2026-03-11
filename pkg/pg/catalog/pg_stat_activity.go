package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
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

func NewPgStatActivityCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
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
	DatID           *int64  `json:"datid" db:"datid"`                       // pg: oid
	DatName         *string `json:"datname" db:"datname"`                   // pg: name
	PID             *int64  `json:"pid" db:"pid"`                           // pg: integer
	LeaderPID       *int64  `json:"leader_pid" db:"leader_pid"`             // pg: integer
	UseSysID        *int64  `json:"usesysid" db:"usesysid"`                 // pg: oid
	UseName         *string `json:"usename" db:"usename"`                   // pg: name
	ApplicationName *string `json:"application_name" db:"application_name"` // pg: text
	ClientAddr      *string `json:"client_addr" db:"client_addr"`           // pg: inet
	ClientHostname  *string `json:"client_hostname" db:"client_hostname"`   // pg: text
	ClientPort      *int64  `json:"client_port" db:"client_port"`           // pg: integer
	BackendStart    *string `json:"backend_start" db:"backend_start"`       // pg: timestamp with time zone
	XactStart       *string `json:"xact_start" db:"xact_start"`             // pg: timestamp with time zone
	QueryStart      *string `json:"query_start" db:"query_start"`           // pg: timestamp with time zone
	StateChange     *string `json:"state_change" db:"state_change"`         // pg: timestamp with time zone
	WaitEventType   *string `json:"wait_event_type" db:"wait_event_type"`   // pg: text
	WaitEvent       *string `json:"wait_event" db:"wait_event"`             // pg: text
	State           *string `json:"state" db:"state"`                       // pg: text
	BackendXID      *string `json:"backend_xid" db:"backend_xid"`           // pg: xid
	BackendXmin     *string `json:"backend_xmin" db:"backend_xmin"`         // pg: xid
	QueryID         *int64  `json:"query_id" db:"query_id"`                 // pg: bigint
	Query           *string `json:"query" db:"query"`                       // pg: text
	BackendType     *string `json:"backend_type" db:"backend_type"`         // pg: text
}

type PgStatActivityPayload struct {
	Rows []PgStatActivityRow `json:"rows"`
}
