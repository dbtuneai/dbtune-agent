package agent

// PgStatioUserIndexesRow represents a row from pg_statio_user_indexes.
type PgStatioUserIndexesRow struct {
	RelID        *int64  `json:"relid" db:"relid"`
	IndexRelID   *int64  `json:"indexrelid" db:"indexrelid"`
	SchemaName   *string `json:"schemaname" db:"schemaname"`
	RelName      *string `json:"relname" db:"relname"`
	IndexRelName *string `json:"indexrelname" db:"indexrelname"`
	IdxBlksRead  *int64  `json:"idx_blks_read" db:"idx_blks_read"`
	IdxBlksHit   *int64  `json:"idx_blks_hit" db:"idx_blks_hit"`
}

type PgStatioUserIndexesPayload struct {
	Rows []PgStatioUserIndexesRow `json:"rows"`
}
