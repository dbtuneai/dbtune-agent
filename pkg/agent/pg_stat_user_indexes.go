package agent

// PgStatUserIndexesRow represents a row from pg_stat_user_indexes.
type PgStatUserIndexesRow struct {
	RelID        *int64  `json:"relid" db:"relid"`
	IndexRelID   *int64  `json:"indexrelid" db:"indexrelid"`
	SchemaName   *string `json:"schemaname" db:"schemaname"`
	RelName      *string `json:"relname" db:"relname"`
	IndexRelName *string `json:"indexrelname" db:"indexrelname"`
	IdxScan      *int64  `json:"idx_scan" db:"idx_scan"`
	LastIdxScan  *string `json:"last_idx_scan" db:"last_idx_scan"`
	IdxTupRead   *int64  `json:"idx_tup_read" db:"idx_tup_read"`
	IdxTupFetch  *int64  `json:"idx_tup_fetch" db:"idx_tup_fetch"`
}

type PgStatUserIndexesPayload struct {
	Rows []PgStatUserIndexesRow `json:"rows"`
}
