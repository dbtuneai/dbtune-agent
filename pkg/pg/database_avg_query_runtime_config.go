package pg

import (
	"fmt"

	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg/queries"
)

// DatabaseAvgQueryRuntimeConfig holds configuration for the
// database_average_query_runtime metric collector.
type DatabaseAvgQueryRuntimeConfig struct {
	IncludeQueries     bool
	MaxQueryTextLength int
}

// DefaultDatabaseAvgQueryRuntimeConfig returns the default configuration.
var DefaultDatabaseAvgQueryRuntimeConfig = DatabaseAvgQueryRuntimeConfig{
	IncludeQueries:     false,
	MaxQueryTextLength: queries.MaxQueryTextLength,
}

func parseDatabaseAvgQueryRuntimeConfig(raw map[string]any) (any, error) {
	cfg := DefaultDatabaseAvgQueryRuntimeConfig
	if v, ok := raw["include_queries"]; ok {
		b, err := collectorconfig.ParseBoolValue(v)
		if err != nil {
			return nil, fmt.Errorf("include_queries: %w", err)
		}
		cfg.IncludeQueries = b
	}
	if v, ok := raw["max_query_text_length"]; ok {
		n, err := collectorconfig.ParseIntValue(v)
		if err != nil {
			return nil, fmt.Errorf("max_query_text_length: %w", err)
		}
		if n < 0 || n > queries.MaxQueryTextLength {
			return nil, fmt.Errorf("max_query_text_length must be between 0 and %d", queries.MaxQueryTextLength)
		}
		cfg.MaxQueryTextLength = n
	}
	return cfg, nil
}

// DatabaseAvgQueryRuntimeRegistration describes the database_average_query_runtime
// collector's configuration schema.
var DatabaseAvgQueryRuntimeRegistration = collectorconfig.CollectorRegistration{
	Name:          "database_average_query_runtime",
	Kind:          collectorconfig.MetricCollectorKind,
	AllowedFields: []string{"include_queries", "max_query_text_length"},
	ParseConfig:   parseDatabaseAvgQueryRuntimeConfig,
}
