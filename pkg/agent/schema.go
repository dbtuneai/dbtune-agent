package agent

const (
	MaxDDLTableColumns     = 5000
	MaxDDLIndexDefinitions = 5000
)

type SchemaColumn struct {
	TableID    int64  `json:"table_id"`
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
	DataType   string `json:"data_type"`
}

type SchemaIndex struct {
	IndexID         int64  `json:"index_id"`
	IndexDefinition string `json:"index_definition"`
}

type SchemaSnapshot struct {
	SchemaHash      string         `json:"schema_hash"`
	TableCount      int            `json:"table_count"`
	IndexCount      int            `json:"index_count"`
	ConstraintCount int            `json:"constraint_count"`
	Tables          []SchemaColumn `json:"tables,omitempty"`
	Indexes         []SchemaIndex  `json:"indexes,omitempty"`
}
