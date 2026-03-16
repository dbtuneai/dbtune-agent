package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION

import (
	"context"
	"time"

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

func PgStatReplicationCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
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
	PID             *Integer     `json:"pid" db:"pid"`
	UseSysID        *Oid         `json:"usesysid" db:"usesysid"`
	UseName         *Name        `json:"usename" db:"usename"`
	ApplicationName *Text        `json:"application_name" db:"application_name"`
	ClientAddr      *Inet        `json:"client_addr" db:"client_addr"`
	ClientHostname  *Text        `json:"client_hostname" db:"client_hostname"`
	ClientPort      *Integer     `json:"client_port" db:"client_port"`
	BackendStart    *TimestampTZ `json:"backend_start" db:"backend_start"`
	BackendXmin     *Xid         `json:"backend_xmin" db:"backend_xmin"`
	State           *Text        `json:"state" db:"state"`
	SentLsn         *PgLsn       `json:"sent_lsn" db:"sent_lsn"`
	WriteLsn        *PgLsn       `json:"write_lsn" db:"write_lsn"`
	FlushLsn        *PgLsn       `json:"flush_lsn" db:"flush_lsn"`
	ReplayLsn       *PgLsn       `json:"replay_lsn" db:"replay_lsn"`
	WriteLag        *Interval    `json:"write_lag" db:"write_lag"`
	FlushLag        *Interval    `json:"flush_lag" db:"flush_lag"`
	ReplayLag       *Interval    `json:"replay_lag" db:"replay_lag"`
	SyncPriority    *Integer     `json:"sync_priority" db:"sync_priority"`
	SyncState       *Text        `json:"sync_state" db:"sync_state"`
	ReplyTime       *TimestampTZ `json:"reply_time" db:"reply_time"`
}

type PgStatReplicationPayload struct {
	Rows []PgStatReplicationRow `json:"rows"`
}
