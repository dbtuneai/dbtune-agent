package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatReplicationName     = "pg_stat_replication"
	PgStatReplicationInterval = 1 * time.Minute
)

const pgStatReplicationQuery = `SELECT * FROM pg_stat_replication`

func CollectPgStatReplication(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatReplicationRow, error) {
	return CollectView[PgStatReplicationRow](pgPool, ctx, pgStatReplicationQuery, "pg_stat_replication")
}

func NewPgStatReplicationCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatReplicationName,
		Interval: PgStatReplicationInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatReplication(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatReplicationPayload{Rows: rows}, nil
		},
	}
}

// PgStatReplicationRow represents a row from pg_stat_replication.
type PgStatReplicationRow struct {
	PID             *int64  `json:"pid" db:"pid"`                         // pg: integer
	UseSysID        *int64  `json:"usesysid" db:"usesysid"`              // pg: oid
	UseName         *string `json:"usename" db:"usename"`                // pg: name
	ApplicationName *string `json:"application_name" db:"application_name"` // pg: text
	ClientAddr      *string `json:"client_addr" db:"client_addr"`        // pg: inet
	ClientHostname  *string `json:"client_hostname" db:"client_hostname"` // pg: text
	ClientPort      *int64  `json:"client_port" db:"client_port"`        // pg: integer
	BackendStart    *string `json:"backend_start" db:"backend_start"`    // pg: timestamp with time zone
	BackendXmin     *string `json:"backend_xmin" db:"backend_xmin"`      // pg: xid
	State           *string `json:"state" db:"state"`                    // pg: text
	SentLsn         *string `json:"sent_lsn" db:"sent_lsn"`             // pg: pg_lsn
	WriteLsn        *string `json:"write_lsn" db:"write_lsn"`           // pg: pg_lsn
	FlushLsn        *string `json:"flush_lsn" db:"flush_lsn"`           // pg: pg_lsn
	ReplayLsn       *string `json:"replay_lsn" db:"replay_lsn"`         // pg: pg_lsn
	WriteLag        *string `json:"write_lag" db:"write_lag"`            // pg: interval
	FlushLag        *string `json:"flush_lag" db:"flush_lag"`            // pg: interval
	ReplayLag       *string `json:"replay_lag" db:"replay_lag"`          // pg: interval
	SyncPriority    *int64  `json:"sync_priority" db:"sync_priority"`    // pg: integer
	SyncState       *string `json:"sync_state" db:"sync_state"`          // pg: text
	ReplyTime       *string `json:"reply_time" db:"reply_time"`          // pg: timestamp with time zone
}

type PgStatReplicationPayload struct {
	Rows []PgStatReplicationRow `json:"rows"`
}
