package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-CONFLICTS

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatDatabaseConflictsRow represents a row from pg_stat_database_conflicts.
type PgStatDatabaseConflictsRow struct {
	DatID            *Oid    `json:"datid" db:"datid"`
	DatName          *Name   `json:"datname" db:"datname"`
	ConflTablespace  *Bigint `json:"confl_tablespace" db:"confl_tablespace"`
	ConflLock        *Bigint `json:"confl_lock" db:"confl_lock"`
	ConflSnapshot    *Bigint `json:"confl_snapshot" db:"confl_snapshot"`
	ConflBufferpin   *Bigint `json:"confl_bufferpin" db:"confl_bufferpin"`
	ConflDeadlock    *Bigint `json:"confl_deadlock" db:"confl_deadlock"`
	ConflLogicalSlot *Bigint `json:"confl_active_logicalslot" db:"confl_active_logicalslot"`
}

const (
	PgStatDatabaseConflictsName     = "pg_stat_database_conflicts"
	PgStatDatabaseConflictsInterval = 1 * time.Minute
)

const pgStatDatabaseConflictsQuery = `SELECT * FROM pg_stat_database_conflicts WHERE datname = current_database()`

// PgStatDatabaseConflictsRegistration describes the pgstatdatabaseconflicts collector's configuration schema.
var PgStatDatabaseConflictsRegistration = collectorconfig.CollectorRegistration{
	Name: PgStatDatabaseConflictsName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgStatDatabaseConflictsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgStatDatabaseConflictsRow](pool, prepareCtx, PgStatDatabaseConflictsName, PgStatDatabaseConflictsInterval, pgStatDatabaseConflictsQuery, WithSkipUnchanged())
}
