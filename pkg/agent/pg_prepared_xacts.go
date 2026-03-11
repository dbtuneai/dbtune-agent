package agent

// PgPreparedXactsRow represents a row from pg_prepared_xacts.
type PgPreparedXactsRow struct {
	Transaction *string `json:"transaction" db:"transaction"`
	GID         *string `json:"gid" db:"gid"`
	Prepared    *string `json:"prepared" db:"prepared"`
	Owner       *string `json:"owner" db:"owner"`
	Database    *string `json:"database" db:"database"`
}

type PgPreparedXactsPayload struct {
	Rows []PgPreparedXactsRow `json:"rows"`
}
