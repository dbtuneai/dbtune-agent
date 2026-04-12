package pg

import (
	"fmt"
	"os"
	"strings"

	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/spf13/viper"
)

// CollectorsConfig is an alias for collectorconfig.CollectorsConfig.
type CollectorsConfig = collectorconfig.CollectorsConfig

// CollectorEntry is an alias for collectorconfig.CollectorEntry.
type CollectorEntry = collectorconfig.CollectorEntry

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

// metricRegistrations defines the configuration schema for metric collectors.
// These live here because the metric collector implementations are in pkg/pg/.
var metricRegistrations = []collectorconfig.CollectorRegistration{
	{
		Name:          "database_average_query_runtime",
		Kind:          collectorconfig.MetricCollectorKind,
		AllowedFields: []string{"include_queries", "max_query_text_length"},
		ParseConfig:   parseDatabaseAvgQueryRuntimeConfig,
	},
	{Name: "database_transactions_per_second", Kind: collectorconfig.MetricCollectorKind},
	{Name: "database_connections", Kind: collectorconfig.MetricCollectorKind | collectorconfig.CatalogCollectorKind},
	{Name: "system_db_size", Kind: collectorconfig.MetricCollectorKind | collectorconfig.CatalogCollectorKind},
	{Name: "database_autovacuum_count", Kind: collectorconfig.MetricCollectorKind},
	{Name: "server_uptime", Kind: collectorconfig.MetricCollectorKind | collectorconfig.CatalogCollectorKind},
	{Name: "pg_database", Kind: collectorconfig.MetricCollectorKind | collectorconfig.CatalogCollectorKind},
	{Name: "pg_user_tables", Kind: collectorconfig.MetricCollectorKind},
	{Name: "pg_bgwriter", Kind: collectorconfig.MetricCollectorKind},
	{Name: "pg_wal", Kind: collectorconfig.MetricCollectorKind},
	{Name: "database_wait_events", Kind: collectorconfig.MetricCollectorKind},
	{Name: "hardware", Kind: collectorconfig.MetricCollectorKind},
	{Name: "pg_checkpointer", Kind: collectorconfig.MetricCollectorKind},
	{Name: "cpu_utilization", Kind: collectorconfig.MetricCollectorKind},
	{Name: "memory_used", Kind: collectorconfig.MetricCollectorKind},
}

// allRegistrations merges catalog and metric collector registrations
// into a lookup map keyed by collector name.
func allRegistrations() map[string]collectorconfig.CollectorRegistration {
	catalog := queries.CatalogRegistrations()
	m := make(map[string]collectorconfig.CollectorRegistration, len(catalog)+len(metricRegistrations))
	for _, r := range catalog {
		m[r.Name] = r
	}
	for _, r := range metricRegistrations {
		m[r.Name] = r
	}
	return m
}

// universalFields are the field keys that every collector accepts.
var universalFields = map[string]struct{}{
	"enabled":          {},
	"interval_seconds": {},
}

// parseBaseConfig extracts universal fields from a raw map into a BaseConfig.
func parseBaseConfig(raw map[string]any) (collectorconfig.BaseConfig, error) {
	var base collectorconfig.BaseConfig
	if v, ok := raw["enabled"]; ok {
		b, err := collectorconfig.ParseBoolValue(v)
		if err != nil {
			return base, fmt.Errorf("enabled: %w", err)
		}
		base.Enabled = &b
	}
	if v, ok := raw["interval_seconds"]; ok {
		n, err := collectorconfig.ParseIntValue(v)
		if err != nil {
			return base, fmt.Errorf("interval_seconds: %w", err)
		}
		if n < 0 {
			return base, fmt.Errorf("interval_seconds must be >= 0")
		}
		base.IntervalSeconds = &n
	}
	return base, nil
}

// CollectorsConfigFromViper reads the collectors YAML section, overlays
// environment variables with pattern DBT_COLLECTOR_{NAME}_{FIELD},
// validates, and returns the config.
func CollectorsConfigFromViper() (CollectorsConfig, error) {
	registry := allRegistrations()
	cfg := make(CollectorsConfig)

	// Phase 1: parse YAML section
	rawMap := viper.GetStringMap("collectors")
	for name, raw := range rawMap {
		reg, ok := registry[name]
		if !ok {
			return nil, fmt.Errorf("unknown collector %q", name)
		}

		fieldMap, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("collector %q: expected map, got %T", name, raw)
		}

		if err := validateFieldNames(name, reg, fieldMap); err != nil {
			return nil, err
		}

		entry, err := parseCollectorEntry(name, reg, fieldMap)
		if err != nil {
			return nil, err
		}
		cfg[name] = entry
	}

	// Phase 2: overlay environment variables
	collectorEnvs := make(map[string]string)
	for _, env := range os.Environ() {
		if key, value, ok := strings.Cut(env, "="); ok && strings.HasPrefix(key, "DBT_COLLECTOR_") {
			collectorEnvs[key] = value
		}
	}

	for name, reg := range registry {
		envPrefix := "DBT_COLLECTOR_" + strings.ToUpper(name) + "_"

		// Collect env var values for this collector
		envFields := make(map[string]any)
		for envKey, envValue := range collectorEnvs {
			if !strings.HasPrefix(envKey, envPrefix) {
				continue
			}
			suffix := strings.ToLower(envKey[len(envPrefix):])
			envFields[suffix] = envValue
		}

		if len(envFields) == 0 {
			continue
		}

		if err := validateFieldNames(name, reg, envFields); err != nil {
			return nil, err
		}

		// Merge env fields into existing or new entry
		existing := cfg[name]

		// Parse base fields from env
		base, err := parseBaseConfig(envFields)
		if err != nil {
			return nil, fmt.Errorf("env var for collector %q: %w", name, err)
		}
		if base.Enabled != nil {
			existing.Base.Enabled = base.Enabled
		}
		if base.IntervalSeconds != nil {
			existing.Base.IntervalSeconds = base.IntervalSeconds
		}

		// Parse extra fields from env
		extraFields := extraFieldsOnly(envFields)
		if len(extraFields) > 0 && reg.ParseConfig != nil {
			// Merge with any existing extra config by re-parsing everything
			// Start from YAML extra fields, then overlay env fields
			merged := make(map[string]any)

			// Get the existing raw YAML extra fields if present
			if yamlRaw, ok := rawMap[name]; ok {
				if yamlFields, ok := yamlRaw.(map[string]any); ok {
					for k, v := range yamlFields {
						if _, isUniversal := universalFields[k]; !isUniversal {
							merged[k] = v
						}
					}
				}
			}

			// Overlay env fields
			for k, v := range extraFields {
				merged[k] = v
			}

			extra, err := reg.ParseConfig(merged)
			if err != nil {
				return nil, fmt.Errorf("collector %q: %w", name, err)
			}
			existing.Extra = extra
		}

		cfg[name] = existing
	}

	return cfg, nil
}

// validateFieldNames checks that all keys in fieldMap are known for the given collector.
func validateFieldNames(name string, reg collectorconfig.CollectorRegistration, fieldMap map[string]any) error {
	allowed := make(map[string]struct{}, len(reg.AllowedFields)+2)
	for k := range universalFields {
		allowed[k] = struct{}{}
	}
	for _, k := range reg.AllowedFields {
		allowed[k] = struct{}{}
	}
	for key := range fieldMap {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("collector %q: unknown field %q", name, key)
		}
	}
	return nil
}

// parseCollectorEntry parses a raw YAML field map into a CollectorEntry.
func parseCollectorEntry(name string, reg collectorconfig.CollectorRegistration, fieldMap map[string]any) (CollectorEntry, error) {
	base, err := parseBaseConfig(fieldMap)
	if err != nil {
		return CollectorEntry{}, fmt.Errorf("collector %q: %w", name, err)
	}

	var extra any
	extraFields := extraFieldsOnly(fieldMap)
	if len(extraFields) > 0 && reg.ParseConfig != nil {
		extra, err = reg.ParseConfig(extraFields)
		if err != nil {
			return CollectorEntry{}, fmt.Errorf("collector %q: %w", name, err)
		}
	}

	return CollectorEntry{Base: base, Extra: extra}, nil
}

// extraFieldsOnly returns a copy of fieldMap without the universal fields.
func extraFieldsOnly(fieldMap map[string]any) map[string]any {
	extra := make(map[string]any, len(fieldMap))
	for k, v := range fieldMap {
		if _, isUniversal := universalFields[k]; !isUniversal {
			extra[k] = v
		}
	}
	return extra
}
