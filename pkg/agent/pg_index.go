package agent

// PgIndexRow represents a row from pg_index joined with pg_class/pg_namespace.
type PgIndexRow struct {
	SchemaName     *string  `json:"schemaname" db:"schemaname"`
	TableName      *string  `json:"tablename" db:"tablename"`
	IndexName      *string  `json:"indexname" db:"indexname"`
	IndexRelID     *int64   `json:"indexrelid" db:"indexrelid"`
	IndRelID       *int64   `json:"indrelid" db:"indrelid"`
	IndNatts       *int64   `json:"indnatts" db:"indnatts"`
	IndNKeyAtts    *int64   `json:"indnkeyatts" db:"indnkeyatts"`
	IndIsUnique    *bool    `json:"indisunique" db:"indisunique"`
	IndIsPrimary   *bool    `json:"indisprimary" db:"indisprimary"`
	IndIsExclusion *bool    `json:"indisexclusion" db:"indisexclusion"`
	IndImmediate   *bool    `json:"indimmediate" db:"indimmediate"`
	IndIsClustered *bool    `json:"indisclustered" db:"indisclustered"`
	IndIsValid     *bool    `json:"indisvalid" db:"indisvalid"`
	IndCheckXmin   *bool    `json:"indcheckxmin" db:"indcheckxmin"`
	IndIsReady     *bool    `json:"indisready" db:"indisready"`
	IndIsLive      *bool    `json:"indislive" db:"indislive"`
	IndIsReplIdent *bool    `json:"indisreplident" db:"indisreplident"`
	RelTuples      *float64 `json:"reltuples" db:"reltuples"`
	IndexDef       *string  `json:"indexdef" db:"indexdef"`
}

type PgIndexPayload struct {
	Rows []PgIndexRow `json:"rows"`
}
