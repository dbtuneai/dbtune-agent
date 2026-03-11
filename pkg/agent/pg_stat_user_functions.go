package agent

// PgStatUserFunctionsRow represents a row from pg_stat_user_functions.
type PgStatUserFunctionsRow struct {
	FuncID     *int64   `json:"funcid" db:"funcid"`
	SchemaName *string  `json:"schemaname" db:"schemaname"`
	FuncName   *string  `json:"funcname" db:"funcname"`
	Calls      *int64   `json:"calls" db:"calls"`
	TotalTime  *float64 `json:"total_time" db:"total_time"`
	SelfTime   *float64 `json:"self_time" db:"self_time"`
}

type PgStatUserFunctionsPayload struct {
	Rows []PgStatUserFunctionsRow `json:"rows"`
}
