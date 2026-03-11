package agent

// PgClassRow represents a single row from pg_class for user tables,
// providing reltuples and relpages needed for index recommendation.
type PgClassRow struct {
	SchemaName string  `json:"schemaname"`
	RelName    string  `json:"relname"`
	RelTuples  float64 `json:"reltuples"`
	RelPages   int     `json:"relpages"`
}

// PgClassPayload is the JSON body POSTed to /api/v1/agent/pg_class.
type PgClassPayload struct {
	Rows []PgClassRow `json:"rows"`
}
