package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-CONFLICTS

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatDatabaseConflictsName     = "pg_stat_database_conflicts"
	PgStatDatabaseConflictsInterval = 1 * time.Minute
)

const pgStatDatabaseConflictsQuery = `SELECT * FROM pg_stat_database_conflicts`

func CollectPgStatDatabaseConflicts(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatDatabaseConflictsRow, error) {
	return CollectView[PgStatDatabaseConflictsRow](pgPool, ctx, pgStatDatabaseConflictsQuery, "pg_stat_database_conflicts")
}

func NewPgStatDatabaseConflictsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatDatabaseConflictsName,
		Interval: PgStatDatabaseConflictsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatDatabaseConflicts(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatDatabaseConflictsPayload{Rows: rows}, nil
		},
	}
}

// PgStatDatabaseConflictsRow represents a row from pg_stat_database_conflicts.
type PgStatDatabaseConflictsRow struct {
	DatID           *int64  `json:"datid" db:"datid"`
	DatName         *string `json:"datname" db:"datname"`
	ConflTablespace *int64  `json:"confl_tablespace" db:"confl_tablespace"`
	ConflLock       *int64  `json:"confl_lock" db:"confl_lock"`
	ConflSnapshot   *int64  `json:"confl_snapshot" db:"confl_snapshot"`
	ConflBufferpin  *int64  `json:"confl_bufferpin" db:"confl_bufferpin"`
	ConflDeadlock   *int64  `json:"confl_deadlock" db:"confl_deadlock"`
	ConflLogicalSlot *int64 `json:"confl_active_logicalslot" db:"confl_active_logicalslot"`
}

type PgStatDatabaseConflictsPayload struct {
	Rows []PgStatDatabaseConflictsRow `json:"rows"`
}
