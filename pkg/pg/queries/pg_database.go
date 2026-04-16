package queries

// https://www.postgresql.org/docs/current/catalog-pg-database.html

import (
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
	Oid        Oid     `json:"oid" db:"oid"`
	DataName   Name    `json:"datname" db:"datname"`
	IsTemplate Boolean `json:"datistemplate" db:"datistemplate"`
	FrozenXID  Xid     `json:"datfrozenxid" db:"datfrozenxid"`
	MinXID     Xid     `json:"datminmxid" db:"datminmxid"`
}

func PgDatabaseCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgDatabaseRow](pool, prepareCtx, PgDatabaseName, PgDatabaseInterval, pgDatabaseQuery)
}
