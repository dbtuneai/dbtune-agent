package agent

// PgStatioUserTablesRow represents a row from pg_statio_user_tables.
type PgStatioUserTablesRow struct {
	RelID         *int64  `json:"relid" db:"relid"`
	SchemaName    *string `json:"schemaname" db:"schemaname"`
	RelName       *string `json:"relname" db:"relname"`
	HeapBlksRead  *int64  `json:"heap_blks_read" db:"heap_blks_read"`
	HeapBlksHit   *int64  `json:"heap_blks_hit" db:"heap_blks_hit"`
	IdxBlksRead   *int64  `json:"idx_blks_read" db:"idx_blks_read"`
	IdxBlksHit    *int64  `json:"idx_blks_hit" db:"idx_blks_hit"`
	ToastBlksRead *int64  `json:"toast_blks_read" db:"toast_blks_read"`
	ToastBlksHit  *int64  `json:"toast_blks_hit" db:"toast_blks_hit"`
	TidxBlksRead  *int64  `json:"tidx_blks_read" db:"tidx_blks_read"`
	TidxBlksHit   *int64  `json:"tidx_blks_hit" db:"tidx_blks_hit"`
}

type PgStatioUserTablesPayload struct {
	Rows []PgStatioUserTablesRow `json:"rows"`
}
