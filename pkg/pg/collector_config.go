package pg

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// CollectorOverride holds per-collector configuration that can be set via
// the collectors section in dbtune.yaml or via environment variables.
//
// These settings override the global postgresql config (Config.IncludeQueries,
// Config.MaximumQueryTextLength) on a per-collector basis. When both are set,
// the per-collector value takes precedence.
type CollectorOverride struct {
	Enabled            *bool `mapstructure:"enabled"`
	IntervalSeconds    *int  `mapstructure:"interval_seconds"`
	IncludeQueries     *bool `mapstructure:"include_queries"`
	MaxQueryTextLength *int  `mapstructure:"max_query_text_length"`
	DiffLimit          *int  `mapstructure:"diff_limit"`
	IncludeTableData   *bool `mapstructure:"include_table_data"`
	BackfillBatchSize  *int  `mapstructure:"backfill_batch_size"`
	CategoryLimit      *int  `mapstructure:"category_limit"`
}

// CollectorsConfig maps collector names to their overrides.
type CollectorsConfig map[string]CollectorOverride

// IsEnabled returns true if the collector is not explicitly disabled.
func (o CollectorOverride) IsEnabled() bool {
	if o.Enabled == nil {
		return true
	}
	return *o.Enabled
}

// IntervalOr returns the configured interval clamped to at least def.
// Users can slow down collection but not speed it up beyond the default.
func (o CollectorOverride) IntervalOr(def time.Duration) time.Duration {
	if o.IntervalSeconds == nil {
		return def
	}
	configured := time.Duration(*o.IntervalSeconds) * time.Second
	if configured < def {
		return def
	}
	return configured
}

// intOr returns *ptr if non-nil, otherwise def.
func intOr(ptr *int, def int) int {
	if ptr == nil {
		return def
	}
	return *ptr
}

// boolOr returns *ptr if non-nil, otherwise def.
func boolOr(ptr *bool, def bool) bool {
	if ptr == nil {
		return def
	}
	return *ptr
}

const (
	MaxDiffLimit       = 500
	MaxQueryTextLength = 8192
)

type collectorFieldSpec struct {
	key       string
	envSuffix string
	apply     func(*CollectorOverride, any) error
	isSet     func(CollectorOverride) bool
	validate  func(string, CollectorOverride) error
}

type collectorKinds uint8

const (
	metricCollectorKind collectorKinds = 1 << iota
	catalogCollectorKind
)

type collectorSpec struct {
	kinds  collectorKinds
	fields []collectorFieldSpec
}

func (s collectorSpec) field(key string) (collectorFieldSpec, bool) {
	for _, field := range s.fields {
		if field.key == key {
			return field, true
		}
	}
	return collectorFieldSpec{}, false
}

func boolField(
	key string,
	get func(CollectorOverride) *bool,
	set func(*CollectorOverride, *bool),
) collectorFieldSpec {
	return collectorFieldSpec{
		key:       key,
		envSuffix: strings.ToUpper(key),
		apply: func(override *CollectorOverride, raw any) error {
			value, err := parseBoolValue(raw)
			if err != nil {
				return err
			}
			set(override, &value)
			return nil
		},
		isSet: func(override CollectorOverride) bool {
			return get(override) != nil
		},
		validate: func(_ string, _ CollectorOverride) error {
			return nil
		},
	}
}

func intField(
	key string,
	get func(CollectorOverride) *int,
	set func(*CollectorOverride, *int),
	validateValue func(string, int) error,
) collectorFieldSpec {
	return collectorFieldSpec{
		key:       key,
		envSuffix: strings.ToUpper(key),
		apply: func(override *CollectorOverride, raw any) error {
			value, err := parseIntValue(raw)
			if err != nil {
				return err
			}
			set(override, &value)
			return nil
		},
		isSet: func(override CollectorOverride) bool {
			return get(override) != nil
		},
		validate: func(name string, override CollectorOverride) error {
			value := get(override)
			if value == nil {
				return nil
			}
			return validateValue(name, *value)
		},
	}
}

func universalCollectorFields() []collectorFieldSpec {
	return []collectorFieldSpec{
		boolField(
			"enabled",
			func(override CollectorOverride) *bool { return override.Enabled },
			func(override *CollectorOverride, value *bool) { override.Enabled = value },
		),
		intField(
			"interval_seconds",
			func(override CollectorOverride) *int { return override.IntervalSeconds },
			func(override *CollectorOverride, value *int) { override.IntervalSeconds = value },
			func(name string, value int) error {
				if value < 0 {
					return fmt.Errorf("collector %q: interval_seconds must be >= 0", name)
				}
				return nil
			},
		),
	}
}

func collectorFieldSpecs(extraFields ...collectorFieldSpec) []collectorFieldSpec {
	fields := universalCollectorFields()
	fields = append(fields, extraFields...)
	return fields
}

func metricCollectorSpec(extraFields ...collectorFieldSpec) collectorSpec {
	return collectorSpec{
		kinds:  metricCollectorKind,
		fields: collectorFieldSpecs(extraFields...),
	}
}

func catalogCollectorSpec(extraFields ...collectorFieldSpec) collectorSpec {
	return collectorSpec{
		kinds:  catalogCollectorKind,
		fields: collectorFieldSpecs(extraFields...),
	}
}

func sharedCollectorSpec(extraFields ...collectorFieldSpec) collectorSpec {
	return collectorSpec{
		kinds:  metricCollectorKind | catalogCollectorKind,
		fields: collectorFieldSpecs(extraFields...),
	}
}

var knownCollectors = map[string]collectorSpec{
	// Metric collectors
	"database_average_query_runtime": metricCollectorSpec(
		boolField(
			"include_queries",
			func(override CollectorOverride) *bool { return override.IncludeQueries },
			func(override *CollectorOverride, value *bool) { override.IncludeQueries = value },
		),
		intField(
			"max_query_text_length",
			func(override CollectorOverride) *int { return override.MaxQueryTextLength },
			func(override *CollectorOverride, value *int) { override.MaxQueryTextLength = value },
			func(name string, value int) error {
				if value < 0 || value > MaxQueryTextLength {
					return fmt.Errorf("collector %q: max_query_text_length must be between 0 and %d", name, MaxQueryTextLength)
				}
				return nil
			},
		),
	),
	"database_transactions_per_second": metricCollectorSpec(),
	"database_connections":             sharedCollectorSpec(),
	"system_db_size":                   sharedCollectorSpec(),
	"database_autovacuum_count":        metricCollectorSpec(),
	"server_uptime":                    sharedCollectorSpec(),
	"pg_database":                      sharedCollectorSpec(),
	"pg_user_tables":                   metricCollectorSpec(),
	"pg_bgwriter":                      metricCollectorSpec(),
	"pg_wal":                           metricCollectorSpec(),
	"database_wait_events":             metricCollectorSpec(),
	"hardware":                         metricCollectorSpec(),
	"pg_checkpointer":                  metricCollectorSpec(),
	"cpu_utilization":                  metricCollectorSpec(),
	"memory_used":                      metricCollectorSpec(),

	// Catalog collectors
	"autovacuum_count":      catalogCollectorSpec(),
	"database_transactions": catalogCollectorSpec(),
	"pg_attribute":          catalogCollectorSpec(),
	"pg_class": catalogCollectorSpec(
		intField(
			"backfill_batch_size",
			func(override CollectorOverride) *int { return override.BackfillBatchSize },
			func(override *CollectorOverride, value *int) { override.BackfillBatchSize = value },
			func(name string, value int) error {
				if value < 0 {
					return fmt.Errorf("collector %q: backfill_batch_size must be >= 0", name)
				}
				return nil
			},
		),
	),
	"pg_index":                      catalogCollectorSpec(),
	"pg_locks":                      catalogCollectorSpec(),
	"pg_prepared_xacts":             catalogCollectorSpec(),
	"pg_replication_slots":          catalogCollectorSpec(),
	"pg_stat_activity":              catalogCollectorSpec(),
	"pg_stat_archiver":              catalogCollectorSpec(),
	"pg_stat_bgwriter":              catalogCollectorSpec(),
	"pg_stat_checkpointer":          catalogCollectorSpec(),
	"pg_stat_database":              catalogCollectorSpec(),
	"pg_stat_database_conflicts":    catalogCollectorSpec(),
	"pg_stat_io":                    catalogCollectorSpec(),
	"pg_stat_progress_analyze":      catalogCollectorSpec(),
	"pg_stat_progress_create_index": catalogCollectorSpec(),
	"pg_stat_progress_vacuum":       catalogCollectorSpec(),
	"pg_stat_replication":           catalogCollectorSpec(),
	"pg_stat_replication_slots":     catalogCollectorSpec(),
	"pg_stat_recovery_prefetch":     catalogCollectorSpec(),
	"pg_stat_slru":                  catalogCollectorSpec(),
	"pg_stat_statements": catalogCollectorSpec(
		intField(
			"diff_limit",
			func(override CollectorOverride) *int { return override.DiffLimit },
			func(override *CollectorOverride, value *int) { override.DiffLimit = value },
			func(name string, value int) error {
				if value < 0 || value > MaxDiffLimit {
					return fmt.Errorf("collector %q: diff_limit must be between 0 and %d", name, MaxDiffLimit)
				}
				return nil
			},
		),
		boolField(
			"include_queries",
			func(override CollectorOverride) *bool { return override.IncludeQueries },
			func(override *CollectorOverride, value *bool) { override.IncludeQueries = value },
		),
		intField(
			"max_query_text_length",
			func(override CollectorOverride) *int { return override.MaxQueryTextLength },
			func(override *CollectorOverride, value *int) { override.MaxQueryTextLength = value },
			func(name string, value int) error {
				if value < 0 || value > MaxQueryTextLength {
					return fmt.Errorf("collector %q: max_query_text_length must be between 0 and %d", name, MaxQueryTextLength)
				}
				return nil
			},
		),
	),
	"pg_stat_subscription":       catalogCollectorSpec(),
	"pg_stat_subscription_stats": catalogCollectorSpec(),
	"pg_stat_user_functions":     catalogCollectorSpec(),
	"pg_stat_user_indexes": catalogCollectorSpec(
		intField(
			"category_limit",
			func(override CollectorOverride) *int { return override.CategoryLimit },
			func(override *CollectorOverride, value *int) { override.CategoryLimit = value },
			func(name string, value int) error {
				if value < 0 {
					return fmt.Errorf("collector %q: category_limit must be >= 0", name)
				}
				return nil
			},
		),
	),
	"pg_stat_user_tables": catalogCollectorSpec(
		intField(
			"category_limit",
			func(override CollectorOverride) *int { return override.CategoryLimit },
			func(override *CollectorOverride, value *int) { override.CategoryLimit = value },
			func(name string, value int) error {
				if value < 0 {
					return fmt.Errorf("collector %q: category_limit must be >= 0", name)
				}
				return nil
			},
		),
	),
	"pg_stat_wal":          catalogCollectorSpec(),
	"pg_stat_wal_receiver": catalogCollectorSpec(),
	"pg_statio_user_indexes": catalogCollectorSpec(
		intField(
			"backfill_batch_size",
			func(override CollectorOverride) *int { return override.BackfillBatchSize },
			func(override *CollectorOverride, value *int) { override.BackfillBatchSize = value },
			func(name string, value int) error {
				if value < 0 {
					return fmt.Errorf("collector %q: backfill_batch_size must be >= 0", name)
				}
				return nil
			},
		),
	),
	"pg_statio_user_tables": catalogCollectorSpec(),
	"pg_stats": catalogCollectorSpec(
		intField(
			"backfill_batch_size",
			func(override CollectorOverride) *int { return override.BackfillBatchSize },
			func(override *CollectorOverride, value *int) { override.BackfillBatchSize = value },
			func(name string, value int) error {
				if value < 0 {
					return fmt.Errorf("collector %q: backfill_batch_size must be >= 0", name)
				}
				return nil
			},
		),
		boolField(
			"include_table_data",
			func(override CollectorOverride) *bool { return override.IncludeTableData },
			func(override *CollectorOverride, value *bool) { override.IncludeTableData = value },
		),
	),
	"wait_events": catalogCollectorSpec(),
}

func parseBoolValue(raw any) (bool, error) {
	switch value := raw.(type) {
	case bool:
		return value, nil
	case string:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return false, err
		}
		return parsed, nil
	default:
		return false, fmt.Errorf("expected boolean, got %T", raw)
	}
}

func parseIntValue(raw any) (int, error) {
	switch value := raw.(type) {
	case int:
		return value, nil
	case int64:
		return int(value), nil
	case float64:
		if value != float64(int(value)) {
			return 0, fmt.Errorf("expected integer, got %v", value)
		}
		return int(value), nil
	case string:
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("expected integer, got %T", raw)
	}
}

func applyCollectorMap(cfg CollectorsConfig, name string, raw any) error {
	spec, ok := knownCollectors[name]
	if !ok {
		return fmt.Errorf("unknown collector %q", name)
	}

	fieldMap, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("collector %q: expected map, got %T", name, raw)
	}

	override := cfg[name]
	for key, value := range fieldMap {
		field, ok := spec.field(key)
		if !ok {
			return fmt.Errorf("collector %q: unknown field %q", name, key)
		}
		if err := field.apply(&override, value); err != nil {
			return fmt.Errorf("collector %q field %q: %w", name, key, err)
		}
	}
	cfg[name] = override
	return nil
}

// CollectorsConfigFromViper reads the collectors YAML section, overlays
// environment variables with pattern DBT_COLLECTOR_{NAME}_{FIELD},
// validates, and returns the config.
func CollectorsConfigFromViper() (CollectorsConfig, error) {
	cfg := make(CollectorsConfig)

	rawMap := viper.GetStringMap("collectors")
	for name, raw := range rawMap {
		if err := applyCollectorMap(cfg, name, raw); err != nil {
			return nil, err
		}
	}

	collectorEnvs := make(map[string]string)
	for _, env := range os.Environ() {
		if key, value, ok := strings.Cut(env, "="); ok && strings.HasPrefix(key, "DBT_COLLECTOR_") {
			collectorEnvs[key] = value
		}
	}

	for name, spec := range knownCollectors {
		envPrefix := "DBT_COLLECTOR_" + strings.ToUpper(name) + "_"
		override := cfg[name]
		changed := false

		for _, field := range spec.fields {
			envKey := envPrefix + field.envSuffix
			value, ok := collectorEnvs[envKey]
			if !ok {
				continue
			}
			if err := field.apply(&override, value); err != nil {
				return nil, fmt.Errorf("env %s: %w", envKey, err)
			}
			changed = true
		}

		if changed {
			cfg[name] = override
		}
	}

	if err := validateCollectorsConfig(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func validateCollectorsConfig(cfg CollectorsConfig) error {
	var errs []string

	for name, override := range cfg {
		spec, known := knownCollectors[name]
		if !known {
			errs = append(errs, fmt.Sprintf("unknown collector %q", name))
			continue
		}

		allowedFields := make(map[string]struct{}, len(spec.fields))
		for _, field := range spec.fields {
			allowedFields[field.key] = struct{}{}
		}
		for _, field := range allCollectorFields() {
			if _, allowed := allowedFields[field.key]; allowed {
				continue
			}
			if field.isSet(override) {
				errs = append(errs, fmt.Sprintf("collector %q: %s is not a valid field", name, field.key))
			}
		}

		for _, field := range spec.fields {
			if err := field.validate(name, override); err != nil {
				errs = append(errs, err.Error())
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("collectors config validation failed:\n  %s", strings.Join(errs, "\n  "))
	}

	return nil
}

func allCollectorFields() []collectorFieldSpec {
	fields := universalCollectorFields()
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		seen[field.key] = struct{}{}
	}

	for _, spec := range knownCollectors {
		for _, field := range spec.fields {
			if _, ok := seen[field.key]; ok {
				continue
			}
			fields = append(fields, field)
			seen[field.key] = struct{}{}
		}
	}

	return fields
}

func (k collectorKinds) has(kind collectorKinds) bool {
	return k&kind != 0
}
