package queries

// https://www.postgresql.org/docs/current/catalog-pg-database.html

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgDatabaseName     = "pg_database"
	PgDatabaseInterval = 1 * time.Minute
)

const pgDatabaseQuery = `
SELECT
	d.oid,
	d.datname,
	d.datistemplate,
	d.datfrozenxid,
	d.datminmxid
FROM pg_database d
`

// PgDatabaseRow represents a single row from pg_database.
type PgDatabaseRow struct {
	Oid        Oid     `json:"oid"`
	DataName   Name    `json:"datname"`
	IsTemplate Boolean `json:"datistemplate"`
	FrozenXID  Xid     `json:"datfrozenxid"`
	MinXID     Xid     `json:"datminmxid"`
}

// PgDatabaseRegistration describes the pgdatabase collector's configuration schema.
var PgDatabaseRegistration = collectorconfig.CollectorRegistration{
	Name: PgDatabaseName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgDatabaseCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgDatabaseRow](pool, prepareCtx, PgDatabaseName, PgDatabaseInterval, pgDatabaseQuery)
}
