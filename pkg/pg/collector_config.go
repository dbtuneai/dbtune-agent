package pg

import (
	"fmt"
	"os"
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
	// Apply parses raw fields and writes the result into the correct field of cfg.
	// A nil raw map applies defaults.
	Apply func(cfg *CollectorsConfig, raw map[string]any) error
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
		Apply: func(cfg *CollectorsConfig, raw map[string]any) error {
			base, remaining, err := collectorconfig.ParsePartial[collectorconfig.BaseConfig](raw)
			if err != nil {
				return err
			}
			extra, err := collectorconfig.Parse[T](remaining)
			if err != nil {
				return err
			}
			*field(cfg) = collectorconfig.TypedEntry[T]{Base: base, Extra: extra}
			return nil
		},
	}
}

// simpleCollector returns a collectorDef for a collector with no extra config.
func simpleCollector(name string) collectorDef {
	return collectorDef{
		Name: name,
		Apply: func(cfg *CollectorsConfig, raw map[string]any) error {
			base, remaining, err := collectorconfig.ParsePartial[collectorconfig.BaseConfig](raw)
			if err != nil {
				return err
			}
			for k := range remaining {
				return fmt.Errorf("unknown field %q", k)
			}
			cfg.Simple[name] = base
			return nil
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

// CollectorsConfigFromViper reads the collectors YAML section, overlays
// environment variables with pattern DBT_COLLECTOR_{NAME}_{FIELD},
// validates, and returns the config.
func CollectorsConfigFromViper() (CollectorsConfig, error) {
	registry := make(map[string]*collectorDef, len(collectors))
	for i := range collectors {
		registry[collectors[i].Name] = &collectors[i]
	}

	// Collect raw field maps per collector (YAML first, env overlay second).
	rawMaps := make(map[string]map[string]any)

	// Phase 1: YAML
	for name, raw := range viper.GetStringMap("collectors") {
		if _, ok := registry[name]; !ok {
			return CollectorsConfig{}, fmt.Errorf("unknown collector %q", name)
		}
		fieldMap, ok := raw.(map[string]any)
		if !ok {
			return CollectorsConfig{}, fmt.Errorf("collector %q: expected map, got %T", name, raw)
		}
		rawMaps[name] = fieldMap
	}

	// Phase 2: match each DBT_COLLECTOR_* env var to its collector (longest name wins).
	for envKey, envValue := range collectDBTEnvVars() {
		suffix := envKey[len("DBT_COLLECTOR_"):]

		var bestName string
		var bestLen int
		for name := range registry {
			prefix := strings.ToUpper(name) + "_"
			if strings.HasPrefix(suffix, prefix) && len(prefix) > bestLen {
				bestName = name
				bestLen = len(prefix)
			}
		}
		if bestName == "" {
			return CollectorsConfig{}, fmt.Errorf(
				"env var %q does not match any known collector (likely a typo)", envKey)
		}

		fieldKey := strings.ToLower(suffix[bestLen:])
		if rawMaps[bestName] == nil {
			rawMaps[bestName] = make(map[string]any)
		}
		rawMaps[bestName][fieldKey] = envValue
	}

	// Phase 3: parse every collector (nil raw -> defaults only).
	cfg := CollectorsConfig{Simple: make(map[string]collectorconfig.BaseConfig)}
	for _, def := range collectors {
		if err := def.Apply(&cfg, rawMaps[def.Name]); err != nil {
			return CollectorsConfig{}, fmt.Errorf("collector %q: %w", def.Name, err)
		}
	}

	return cfg, nil
}

// collectDBTEnvVars returns all DBT_COLLECTOR_* environment variables.
func collectDBTEnvVars() map[string]string {
	result := make(map[string]string)
	for _, env := range os.Environ() {
		if key, value, ok := strings.Cut(env, "="); ok && strings.HasPrefix(key, "DBT_COLLECTOR_") {
			result[key] = value
		}
	}
	return result
}
