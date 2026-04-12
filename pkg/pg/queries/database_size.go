package queries

// DatabaseSize queries the total size of all databases in the PostgreSQL instance.
//
// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DatabaseSizeRow struct {
	TotalSizeBytes Bigint `json:"total_size_bytes" db:"total_size_bytes"`
}

const (
	DatabaseSizeName     = "system_db_size"
	DatabaseSizeInterval = 5 * time.Second
)

const databaseSizeQuery = `SELECT sum(pg_database_size(datname))::bigint AS total_size_bytes FROM pg_database`

func DatabaseSizeCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[DatabaseSizeRow](pool, prepareCtx, DatabaseSizeName, DatabaseSizeInterval, databaseSizeQuery)
}
