package pg

// DatabaseAvgQueryRuntimeConfig holds configuration for the
// database_average_query_runtime metric collector.
type DatabaseAvgQueryRuntimeConfig struct {
	IncludeQueries     bool `config:"include_queries"`
	MaxQueryTextLength int  `config:"max_query_text_length" default:"8192" min:"0" max:"8192"`
}
