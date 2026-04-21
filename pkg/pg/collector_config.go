package pg

import (
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/spf13/viper"
)

// Metric collector key constants.
// These are the Key values used in adapter MetricCollector entries.
const (
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
	PgStatStatements    collectorconfig.TypedEntry[queries.PgStatStatementsConfig]
	PgClass             collectorconfig.TypedEntry[queries.PgClassConfig]
	PgStats             collectorconfig.TypedEntry[queries.PgStatsConfig]
	PgStatUserTables    collectorconfig.TypedEntry[queries.PgStatUserTablesConfig]
	PgStatUserIndexes   collectorconfig.TypedEntry[queries.PgStatUserIndexesConfig]
	PgStatioUserIndexes collectorconfig.TypedEntry[queries.PgStatioUserIndexesConfig]

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

	// Simple collectors (no extra config beyond BaseConfig)
	simpleCollector(queries.AutovacuumCountName),
	simpleCollector(queries.PgAttributeName),
	simpleCollector(queries.PgConstraintName),
	simpleCollector(queries.PgDatabaseName),
	simpleCollector(queries.PgIndexName),
	simpleCollector(queries.PgLocksName),
	simpleCollector(queries.PgPreparedXactsName),
	simpleCollector(queries.PgReplicationSlotsName),
	simpleCollector(queries.PgTypeName),
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

// envConflictResolutions declares how to resolve env var prefix collisions
// between collector names. When two collector names both match an env var
// suffix, the pair is looked up here to determine which collector wins.
// A test ensures every prefix collision in the registry has an entry here.
var envConflictResolutions = map[[2]string]string{
	{"database_transactions", "database_transactions_per_second"}: "database_transactions_per_second",
	{"pg_stat_database", "pg_stat_database_conflicts"}:            "pg_stat_database_conflicts",
	{"pg_stat_replication", "pg_stat_replication_slots"}:          "pg_stat_replication_slots",
	{"pg_stat_subscription", "pg_stat_subscription_stats"}:        "pg_stat_subscription_stats",
	{"pg_stat_wal", "pg_stat_wal_receiver"}:                       "pg_stat_wal_receiver",
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

	// Phase 2: match each DBT_COLLECTOR_* env var to its collector.
	strictEnv := os.Getenv("DBT_COLLECTOR_STRICT_ENV") != "false"
	for suffix, envValue := range collectDBTCollectorEnvSuffixes() {
		if suffix == "strict_env" {
			continue
		}

		var matches []string
		for name := range registry {
			if strings.HasPrefix(suffix, name+"_") {
				matches = append(matches, name)
			}
		}

		var matchedName string
		switch len(matches) {
		case 0:
			if strictEnv {
				return CollectorsConfig{}, fmt.Errorf(
					"env var DBT_COLLECTOR_%s does not match any known collector (set DBT_COLLECTOR_STRICT_ENV=false to skip unknown env vars)",
					strings.ToUpper(suffix),
				)
			}
			slog.Warn("ignoring unrecognized collector env var", "key", "DBT_COLLECTOR_"+strings.ToUpper(suffix))
			continue
		case 1:
			matchedName = matches[0]
		default:
			// Multiple collectors match — resolve via conflict table.
			slices.Sort(matches)
			key := [2]string{matches[0], matches[1]}
			winner, ok := envConflictResolutions[key]
			if !ok {
				return CollectorsConfig{}, fmt.Errorf(
					"env var DBT_COLLECTOR_%s is ambiguous, matches collectors: %s",
					strings.ToUpper(suffix),
					strings.Join(matches, ", "),
				)
			}
			matchedName = winner
		}

		fieldKey := suffix[len(matchedName)+1:]
		if rawMaps[matchedName] == nil {
			rawMaps[matchedName] = make(map[string]any)
		}
		rawMaps[matchedName][fieldKey] = envValue
	}

	// Phase 2.5: legacy aliases — older configs/installs set query-text options
	// on the top-level postgresql block (YAML or DBT_POSTGRESQL_* env). These
	// settings now live on the pg_stat_statements collector; forward each
	// legacy source if the new one is unset. Docs only show the new form.
	for _, a := range legacyPgStatStatementsAliases {
		value, ok := lookupLegacyValue(a)
		if !ok {
			continue
		}
		if rawMaps[queries.PgStatStatementsName] == nil {
			rawMaps[queries.PgStatStatementsName] = make(map[string]any)
		}
		if _, set := rawMaps[queries.PgStatStatementsName][a.newField]; !set {
			rawMaps[queries.PgStatStatementsName][a.newField] = value
		}
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

// legacyAlias maps a legacy top-level postgresql setting onto a field on the
// pg_stat_statements collector. Precedence: env > postgresql.* YAML >
// postgres.* YAML (older key alias).
type legacyAlias struct {
	envVar   string
	yamlKey  string // under postgresql.* (and postgres.* as older alias)
	newField string // field name on pg_stat_statements collector
}

var legacyPgStatStatementsAliases = []legacyAlias{
	{envVar: "DBT_POSTGRESQL_INCLUDE_QUERIES", yamlKey: "include_queries", newField: "include_queries"},
	{envVar: "DBT_POSTGRESQL_MAXIMUM_QUERY_TEXT_LENGTH", yamlKey: "maximum_query_text_length", newField: "max_query_text_length"},
}

func lookupLegacyValue(a legacyAlias) (any, bool) {
	if v := os.Getenv(a.envVar); v != "" {
		return v, true
	}
	if viper.IsSet("postgresql." + a.yamlKey) {
		return viper.Get("postgresql." + a.yamlKey), true
	}
	if viper.IsSet("postgres." + a.yamlKey) {
		return viper.Get("postgres." + a.yamlKey), true
	}
	return nil, false
}

// collectDBTCollectorEnvSuffixes returns DBT_COLLECTOR_* env vars keyed by
// their lowercased suffix (i.e. with the "DBT_COLLECTOR_" prefix stripped).
func collectDBTCollectorEnvSuffixes() map[string]string {
	const prefix = "DBT_COLLECTOR_"
	result := make(map[string]string)
	for _, env := range os.Environ() {
		if key, value, ok := strings.Cut(env, "="); ok && strings.HasPrefix(key, prefix) {
			result[strings.ToLower(key[len(prefix):])] = value
		}
	}
	return result
}
