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

// Metric collector key constants.
// These are the Key values used in adapter MetricCollector entries.
const (
	MetricDatabaseAvgQueryRuntime       = "database_average_query_runtime"
	MetricDatabaseTransactionsPerSecond = "database_transactions_per_second"
	MetricDatabaseConnections           = "database_connections"
	MetricSystemDbSize                  = "system_db_size"
	MetricDatabaseAutovacuumCount       = "database_autovacuum_count"
	MetricServerUptime                  = "server_uptime"
	MetricPgDatabase                    = "pg_database"
	MetricPgUserTables                  = "pg_user_tables"
	MetricPgBgwriter                    = "pg_bgwriter"
	MetricPgWal                         = "pg_wal"
	MetricDatabaseWaitEvents            = "database_wait_events"
	MetricHardware                      = "hardware"
	MetricPgCheckpointer                = "pg_checkpointer"
	MetricCpuUtilization                = "cpu_utilization"
	MetricMemoryUsed                    = "memory_used"
)

// CollectorsConfig holds the parsed, typed configuration for every collector.
type CollectorsConfig struct {
	PgStatStatements        collectorconfig.TypedEntry[queries.PgStatStatementsConfig]
	PgClass                 collectorconfig.TypedEntry[queries.PgClassConfig]
	PgStats                 collectorconfig.TypedEntry[queries.PgStatsConfig]
	PgStatUserTables        collectorconfig.TypedEntry[queries.PgStatUserTablesConfig]
	PgStatUserIndexes       collectorconfig.TypedEntry[queries.PgStatUserIndexesConfig]
	PgStatioUserIndexes     collectorconfig.TypedEntry[queries.PgStatioUserIndexesConfig]
	DatabaseAvgQueryRuntime collectorconfig.TypedEntry[DatabaseAvgQueryRuntimeConfig]

	// Simple holds config for collectors that only use BaseConfig.
	Simple map[string]collectorconfig.BaseConfig
}

// collectorDef is an entry in the single collector registry.
type collectorDef struct {
	Name string
	// Apply parses extraFields, merges with base, and writes the result
	// into the correct field of cfg. Called with zero inputs to set defaults,
	// then again with parsed values to apply overrides.
	Apply func(cfg *CollectorsConfig, base collectorconfig.BaseConfig, extraFields map[string]any) error
	// GetBase returns the current BaseConfig for this collector from cfg.
	GetBase func(cfg *CollectorsConfig) collectorconfig.BaseConfig
}

// typedCollector returns a collectorDef for a collector with typed extra config.
// T must be a struct with `config` tags — parsed via collectorconfig.Parse[T].
// field returns a pointer to the TypedEntry on CollectorsConfig.
func typedCollector[T any](
	name string,
	field func(*CollectorsConfig) *collectorconfig.TypedEntry[T],
) collectorDef {
	return collectorDef{
		Name: name,
		Apply: func(cfg *CollectorsConfig, base collectorconfig.BaseConfig, extraFields map[string]any) error {
			extra, err := collectorconfig.Parse[T](extraFields)
			if err != nil {
				return err
			}
			*field(cfg) = collectorconfig.TypedEntry[T]{Base: base, Extra: extra}
			return nil
		},
		GetBase: func(cfg *CollectorsConfig) collectorconfig.BaseConfig {
			return field(cfg).Base
		},
	}
}

// simpleCollector returns a collectorDef for a collector with no extra config.
func simpleCollector(name string) collectorDef {
	return collectorDef{
		Name: name,
		Apply: func(cfg *CollectorsConfig, base collectorconfig.BaseConfig, extraFields map[string]any) error {
			for k := range extraFields {
				return fmt.Errorf("unknown field %q", k)
			}
			cfg.Simple[name] = base
			return nil
		},
		GetBase: func(cfg *CollectorsConfig) collectorconfig.BaseConfig {
			return cfg.Simple[name]
		},
	}
}

// collectors is the single registry of all known collectors.
// This is the sole source of truth for collector names and parsing.
var collectors = []collectorDef{
	// Typed collectors — name and field accessor. Config parsed via struct tags.
	typedCollector[queries.PgStatStatementsConfig](queries.PgStatStatementsName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[queries.PgStatStatementsConfig] {
			return &c.PgStatStatements
		}),
	typedCollector[queries.PgClassConfig](queries.PgClassName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[queries.PgClassConfig] { return &c.PgClass }),
	typedCollector[queries.PgStatsConfig](queries.PgStatsName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[queries.PgStatsConfig] { return &c.PgStats }),
	typedCollector[queries.PgStatUserTablesConfig](queries.PgStatUserTablesName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[queries.PgStatUserTablesConfig] {
			return &c.PgStatUserTables
		}),
	typedCollector[queries.PgStatUserIndexesConfig](queries.PgStatUserIndexesName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[queries.PgStatUserIndexesConfig] {
			return &c.PgStatUserIndexes
		}),
	typedCollector[queries.PgStatioUserIndexesConfig](queries.PgStatioUserIndexesName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[queries.PgStatioUserIndexesConfig] {
			return &c.PgStatioUserIndexes
		}),
	typedCollector[DatabaseAvgQueryRuntimeConfig](MetricDatabaseAvgQueryRuntime,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[DatabaseAvgQueryRuntimeConfig] {
			return &c.DatabaseAvgQueryRuntime
		}),

	// Simple collectors (no extra config beyond BaseConfig)
	simpleCollector(queries.AutovacuumCountName),
	simpleCollector(queries.PgAttributeName),
	simpleCollector(queries.PgDatabaseName),
	simpleCollector(queries.PgIndexName),
	simpleCollector(queries.PgLocksName),
	simpleCollector(queries.PgPreparedXactsName),
	simpleCollector(queries.PgReplicationSlotsName),
	simpleCollector(queries.PgStatActivityName),
	simpleCollector(queries.PgStatArchiverName),
	simpleCollector(queries.PgStatBgwriterName),
	simpleCollector(queries.PgStatCheckpointerName),
	simpleCollector(queries.PgStatDatabaseName),
	simpleCollector(queries.PgStatDatabaseConflictsName),
	simpleCollector(queries.PgStatIOName),
	simpleCollector(queries.PgStatProgressAnalyzeName),
	simpleCollector(queries.PgStatProgressCreateIndexName),
	simpleCollector(queries.PgStatProgressVacuumName),
	simpleCollector(queries.PgStatRecoveryPrefetchName),
	simpleCollector(queries.PgStatReplicationName),
	simpleCollector(queries.PgStatReplicationSlotsName),
	simpleCollector(queries.PgStatSlruName),
	simpleCollector(queries.PgStatSubscriptionName),
	simpleCollector(queries.PgStatSubscriptionStatsName),
	simpleCollector(queries.PgStatUserFunctionsName),
	simpleCollector(queries.PgStatWalName),
	simpleCollector(queries.PgStatWalReceiverName),
	simpleCollector(queries.PgStatioUserTablesName),
	simpleCollector(queries.TransactionCommitsName),
	simpleCollector(queries.UptimeMinutesName),
	simpleCollector(queries.WaitEventsName),
	simpleCollector(MetricDatabaseConnections),
	simpleCollector(MetricSystemDbSize),
	simpleCollector(MetricServerUptime),
	simpleCollector(MetricDatabaseTransactionsPerSecond),
	simpleCollector(MetricDatabaseAutovacuumCount),
	simpleCollector(MetricPgUserTables),
	simpleCollector(MetricPgBgwriter),
	simpleCollector(MetricPgWal),
	simpleCollector(MetricDatabaseWaitEvents),
	simpleCollector(MetricHardware),
	simpleCollector(MetricPgCheckpointer),
	simpleCollector(MetricCpuUtilization),
	simpleCollector(MetricMemoryUsed),
}

// universalFields are the field keys that every collector accepts.
var universalFields = map[string]bool{
	"enabled":          true,
	"interval_seconds": true,
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

// extraFieldsOnly returns a copy of fieldMap without the universal fields.
func extraFieldsOnly(fieldMap map[string]any) map[string]any {
	extra := make(map[string]any, len(fieldMap))
	for k, v := range fieldMap {
		if !universalFields[k] {
			extra[k] = v
		}
	}
	return extra
}

// CollectorsConfigFromViper reads the collectors YAML section, overlays
// environment variables with pattern DBT_COLLECTOR_{NAME}_{FIELD},
// validates, and returns the config.
func CollectorsConfigFromViper() (CollectorsConfig, error) {
	// Build name→def lookup from the single registry.
	registry := make(map[string]*collectorDef, len(collectors))
	for i := range collectors {
		registry[collectors[i].Name] = &collectors[i]
	}

	cfg := CollectorsConfig{Simple: make(map[string]collectorconfig.BaseConfig)}

	// Initialize all collectors with their defaults (zero BaseConfig + default Extra).
	for _, def := range collectors {
		if err := def.Apply(&cfg, collectorconfig.BaseConfig{}, nil); err != nil {
			return CollectorsConfig{}, fmt.Errorf("collector %q default init: %w", def.Name, err)
		}
	}

	// yamlExtraFields stores the raw extra (non-universal) fields from YAML
	// for each collector, so Phase 2 can merge env overlays without reaching
	// back into the viper raw map.
	yamlExtraFields := make(map[string]map[string]any)

	// Phase 1: parse YAML section
	rawMap := viper.GetStringMap("collectors")
	for name, raw := range rawMap {
		def, ok := registry[name]
		if !ok {
			return CollectorsConfig{}, fmt.Errorf("unknown collector %q", name)
		}

		fieldMap, ok := raw.(map[string]any)
		if !ok {
			return CollectorsConfig{}, fmt.Errorf("collector %q: expected map, got %T", name, raw)
		}

		base, err := parseBaseConfig(fieldMap)
		if err != nil {
			return CollectorsConfig{}, fmt.Errorf("collector %q: %w", name, err)
		}

		extra := extraFieldsOnly(fieldMap)
		if err := def.Apply(&cfg, base, extra); err != nil {
			return CollectorsConfig{}, fmt.Errorf("collector %q: %w", name, err)
		}

		yamlExtraFields[name] = extra
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
		def := registry[name]
		envPrefix := "DBT_COLLECTOR_" + strings.ToUpper(name) + "_"

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

		// Parse base fields from env
		base, err := parseBaseConfig(envFields)
		if err != nil {
			return CollectorsConfig{}, fmt.Errorf("env var for collector %q: %w", name, err)
		}

		// For the typed entry, we need the current base and merge env extra on
		// top of any YAML extra fields.
		existingBase := def.GetBase(&cfg)
		if base.Enabled != nil {
			existingBase.Enabled = base.Enabled
		}
		if base.IntervalSeconds != nil {
			existingBase.IntervalSeconds = base.IntervalSeconds
		}

		// Merge YAML extra fields with env extra fields
		merged := make(map[string]any)
		for k, v := range yamlExtraFields[name] {
			merged[k] = v
		}
		for k, v := range extraFieldsOnly(envFields) {
			merged[k] = v
		}

		if err := def.Apply(&cfg, existingBase, merged); err != nil {
			return CollectorsConfig{}, fmt.Errorf("collector %q: %w", name, err)
		}
	}

	// Any remaining DBT_COLLECTOR_* env vars didn't match a known collector.
	for envKey := range collectorEnvs {
		return CollectorsConfig{}, fmt.Errorf("env var %q does not match any known collector (likely a typo)", envKey)
	}

	return cfg, nil
}
