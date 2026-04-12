package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-USER-FUNCTIONS

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatUserFunctionsRow represents a row from pg_stat_user_functions.
type PgStatUserFunctionsRow struct {
	FuncID     *Oid             `json:"funcid" db:"funcid"`
	SchemaName *Name            `json:"schemaname" db:"schemaname"`
	FuncName   *Name            `json:"funcname" db:"funcname"`
	Calls      *Bigint          `json:"calls" db:"calls"`
	TotalTime  *DoublePrecision `json:"total_time" db:"total_time"`
	SelfTime   *DoublePrecision `json:"self_time" db:"self_time"`
}

const (
	PgStatUserFunctionsName     = "pg_stat_user_functions"
	PgStatUserFunctionsInterval = 1 * time.Minute
)

const pgStatUserFunctionsQuery = `
SELECT * FROM pg_stat_user_functions
WHERE calls > 0
ORDER BY calls DESC
LIMIT 500
`

// PgStatUserFunctionsRegistration describes the pgstatuserfunctions collector's configuration schema.
var PgStatUserFunctionsRegistration = collectorconfig.CollectorRegistration{
	Name: PgStatUserFunctionsName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgStatUserFunctionsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgStatUserFunctionsRow](pool, prepareCtx, PgStatUserFunctionsName, PgStatUserFunctionsInterval, pgStatUserFunctionsQuery)
}
