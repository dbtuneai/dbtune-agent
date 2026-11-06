package catalog

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/dbtuneai/agent/pkg/catalog/collectorconfig"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Collector names left from before Postgres metrics moved from
// MetricCollectors to CatalogCollectors. They stay in the collector registry
// below so configs and DBT_COLLECTOR_* env vars that name them still parse.
// MetricHardware is still the Key of each adapter's hardware metrics.Collector.
// Some names are also a catalog collector's name (database_connections,
// system_db_size, server_uptime), so their config applies to that collector;
// config under the other names is accepted and ignored.
const (
	MetricDatabaseTransactionsPerSecond = "database_transactions_per_second"
	MetricDatabaseConnections           = "database_connections"
	MetricSystemDbSize                  = "system_db_size"
	MetricDatabaseAutovacuumCount       = "database_autovacuum_count"
	MetricServerUptime                  = "server_uptime"
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
	PgStatStatements    collectorconfig.TypedEntry[PgStatStatementsConfig]
	PgClass             collectorconfig.TypedEntry[PgClassConfig]
	PgStats             collectorconfig.TypedEntry[PgStatsConfig]
	PgStatUserTables    collectorconfig.TypedEntry[PgStatUserTablesConfig]
	PgStatUserIndexes   collectorconfig.TypedEntry[PgStatUserIndexesConfig]
	PgStatioUserIndexes collectorconfig.TypedEntry[PgStatioUserIndexesConfig]

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
	typedCollector[PgStatStatementsConfig](PgStatStatementsName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[PgStatStatementsConfig] {
			return &c.PgStatStatements
		}),
	typedCollector[PgClassConfig](PgClassName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[PgClassConfig] { return &c.PgClass }),
	typedCollector[PgStatsConfig](PgStatsName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[PgStatsConfig] { return &c.PgStats }),
	typedCollector[PgStatUserTablesConfig](PgStatUserTablesName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[PgStatUserTablesConfig] {
			return &c.PgStatUserTables
		}),
	typedCollector[PgStatUserIndexesConfig](PgStatUserIndexesName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[PgStatUserIndexesConfig] {
			return &c.PgStatUserIndexes
		}),
	typedCollector[PgStatioUserIndexesConfig](PgStatioUserIndexesName,
		func(c *CollectorsConfig) *collectorconfig.TypedEntry[PgStatioUserIndexesConfig] {
			return &c.PgStatioUserIndexes
		}),

	// Simple collectors (no extra config beyond BaseConfig)
	simpleCollector(AutovacuumCountName),
	simpleCollector(CapabilitiesName),
	simpleCollector(PgAttributeName),
	simpleCollector(PgConstraintName),
	simpleCollector(PgDatabaseName),
	simpleCollector(PgIndexName),
	simpleCollector(PgLocksName),
	simpleCollector(PgPreparedXactsName),
	simpleCollector(PgReplicationSlotsName),
	simpleCollector(PgTypeName),
	simpleCollector(PgStatActivityName),
	simpleCollector(PgStatArchiverName),
	simpleCollector(PgStatBgwriterName),
	simpleCollector(PgStatCheckpointerName),
	simpleCollector(PgStatDatabaseName),
	simpleCollector(PgStatDatabaseConflictsName),
	simpleCollector(PgStatIOName),
	simpleCollector(PgStatProgressAnalyzeName),
	simpleCollector(PgStatProgressCreateIndexName),
	simpleCollector(PgStatProgressVacuumName),
	simpleCollector(PgStatRecoveryPrefetchName),
	simpleCollector(PgStatReplicationName),
	simpleCollector(PgStatReplicationSlotsName),
	simpleCollector(PgStatSlruName),
	simpleCollector(PgStatSubscriptionName),
	simpleCollector(PgStatSubscriptionStatsName),
	simpleCollector(PgStatUserFunctionsName),
	simpleCollector(PgStatWalName),
	simpleCollector(PgStatWalReceiverName),
	simpleCollector(PgStatioUserTablesName),
	simpleCollector(TransactionCommitsName),
	simpleCollector(UptimeMinutesName),
	simpleCollector(WaitEventsName),
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
func CollectorsConfigFromViper(logger *logrus.Logger) (CollectorsConfig, error) {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
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
			logger.Warnf("ignoring unrecognized collector env var DBT_COLLECTOR_%s", strings.ToUpper(suffix))
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
		if rawMaps[PgStatStatementsName] == nil {
			rawMaps[PgStatStatementsName] = make(map[string]any)
		}
		if _, set := rawMaps[PgStatStatementsName][a.newField]; !set {
			rawMaps[PgStatStatementsName][a.newField] = value
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
