package pg

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/spf13/viper"
)

// CollectorsConfig is an alias for collectorconfig.CollectorsConfig.
type CollectorsConfig = collectorconfig.CollectorsConfig

// CollectorEntry is an alias for collectorconfig.CollectorEntry.
type CollectorEntry = collectorconfig.CollectorEntry

// metricRegistrations defines the configuration schema for metric collectors.
// These live here because the metric collector implementations are in pkg/pg/.
var metricRegistrations = []collectorconfig.CollectorRegistration{
	DatabaseAvgQueryRuntimeRegistration,
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

	// yamlExtraFields stores the raw extra (non-universal) fields from YAML
	// for each collector, so Phase 2 can merge env overlays without reaching
	// back into the viper raw map.
	yamlExtraFields := make(map[string]map[string]any)

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

		// Stash raw extra fields for potential env-var merging in Phase 2.
		yamlExtraFields[name] = extraFieldsOnly(fieldMap)
	}

	// Phase 2: overlay environment variables
	collectorEnvs := make(map[string]string)
	for _, env := range os.Environ() {
		if key, value, ok := strings.Cut(env, "="); ok && strings.HasPrefix(key, "DBT_COLLECTOR_") {
			collectorEnvs[key] = value
		}
	}

	// Sort collector names longest-first so that "pg_stat_wal_receiver" is
	// matched before "pg_stat_wal". Without this, a shared prefix could
	// cause an env var to bind to the wrong collector.
	sortedNames := make([]string, 0, len(registry))
	for name := range registry {
		sortedNames = append(sortedNames, name)
	}
	sort.Slice(sortedNames, func(i, j int) bool {
		return len(sortedNames[i]) > len(sortedNames[j])
	})

	for _, name := range sortedNames {
		reg := registry[name]
		envPrefix := "DBT_COLLECTOR_" + strings.ToUpper(name) + "_"

		// Collect env var values for this collector
		envFields := make(map[string]any)
		for envKey, envValue := range collectorEnvs {
			if !strings.HasPrefix(envKey, envPrefix) {
				continue
			}
			suffix := strings.ToLower(envKey[len(envPrefix):])
			envFields[suffix] = envValue
			// Remove from the map so it isn't re-matched by a shorter prefix.
			delete(collectorEnvs, envKey)
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
		extra := extraFieldsOnly(envFields)
		if len(extra) > 0 && reg.ParseConfig != nil {
			// Start from stashed YAML extra fields, then overlay env fields.
			merged := make(map[string]any)
			for k, v := range yamlExtraFields[name] {
				merged[k] = v
			}
			for k, v := range extra {
				merged[k] = v
			}

			parsed, err := reg.ParseConfig(merged)
			if err != nil {
				return nil, fmt.Errorf("collector %q: %w", name, err)
			}
			existing.Extra = parsed
		}

		cfg[name] = existing
	}

	// Any remaining DBT_COLLECTOR_* env vars didn't match a known collector.
	for envKey := range collectorEnvs {
		return nil, fmt.Errorf("env var %q does not match any known collector (likely a typo)", envKey)
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
