package queries

// Autovacuum counts the number of currently running autovacuum worker processes
// by filtering pg_stat_activity for queries that start with 'autovacuum:'.
//
// https://www.postgresql.org/docs/current/routine-vacuuming.html#AUTOVACUUM

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AutovacuumCountRow struct {
	Count Bigint `json:"count" db:"count"`
}

const (
	AutovacuumCountName     = "autovacuum_count"
	AutovacuumCountInterval = 5 * time.Second
)

const autovacuumQuery = `SELECT COUNT(*) AS count FROM pg_stat_activity WHERE starts_with(query, 'autovacuum:')`

// AutovacuumCountRegistration describes the autovacuumcount collector's configuration schema.
var AutovacuumCountRegistration = collectorconfig.CollectorRegistration{
	Name: AutovacuumCountName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func AutovacuumCountCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[AutovacuumCountRow](pool, prepareCtx, AutovacuumCountName, AutovacuumCountInterval, autovacuumQuery)
}
