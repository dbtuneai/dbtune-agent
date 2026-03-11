package agent

// DDLPayload is the JSON body POSTed to /api/v1/agent/ddl.
type DDLPayload struct {
	DDL  string `json:"ddl"`
	Hash string `json:"ddl_hash"`
}
