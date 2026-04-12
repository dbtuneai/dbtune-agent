package queries

// https://www.postgresql.org/docs/current/view-pg-prepared-xacts.html

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgPreparedXactsRow represents a row from pg_prepared_xacts.
type PgPreparedXactsRow struct {
	Transaction *Xid         `json:"transaction" db:"transaction"`
	GID         *Text        `json:"gid" db:"gid"`
	Prepared    *TimestampTZ `json:"prepared" db:"prepared"`
	Owner       *Name        `json:"owner" db:"owner"`
	Database    *Name        `json:"database" db:"database"`
}

const (
	PgPreparedXactsName     = "pg_prepared_xacts"
	PgPreparedXactsInterval = 1 * time.Minute
)

const pgPreparedXactsQuery = `SELECT * FROM pg_prepared_xacts WHERE database = current_database() ORDER BY gid`

// PgPreparedXactsRegistration describes the pgpreparedxacts collector's configuration schema.
var PgPreparedXactsRegistration = collectorconfig.CollectorRegistration{
	Name: PgPreparedXactsName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgPreparedXactsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgPreparedXactsRow](pool, prepareCtx, PgPreparedXactsName, PgPreparedXactsInterval, pgPreparedXactsQuery, WithSkipUnchanged())
}
