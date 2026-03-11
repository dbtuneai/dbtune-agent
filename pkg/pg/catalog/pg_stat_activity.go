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

const pgStatActivityQuery = `SELECT * FROM pg_stat_activity`

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
