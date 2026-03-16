package pg

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// CollectorsConfigFromViper reads the top-level "collectors" YAML section,
// binds env vars for each known collector, and returns the parsed config.
// Returns an empty (non-nil) map if the section is missing.
func CollectorsConfigFromViper() (CollectorsConfig, error) {
	cfg := make(CollectorsConfig)

	// Bind env vars for all known collectors before reading.
	// Pattern: DBT_COLLECTOR_{UPPERCASE_NAME}_{FIELD}
	universalFields := []string{"enabled", "interval_seconds"}
	for name, extraFields := range knownCollectorFields {
		envName := strings.ToUpper(strings.ReplaceAll(name, " ", "_"))
		allFields := append(universalFields, extraFields...)
		for _, field := range allFields {
			envKey := fmt.Sprintf("DBT_COLLECTOR_%s_%s", envName, strings.ToUpper(field))
			viperKey := fmt.Sprintf("collectors.%s.%s", name, field)
			_ = viper.BindEnv(viperKey, envKey)
		}
	}

	sub := viper.Sub("collectors")
	if sub == nil {
		// No collectors section — check if any env vars were set by reading
		// from the global viper instance.
		for name := range knownCollectorFields {
			prefix := fmt.Sprintf("collectors.%s", name)
			settings := viper.AllSettings()
			if collectorsMap, ok := settings["collectors"]; ok {
				if cm, ok := collectorsMap.(map[string]any); ok {
					if collectorMap, ok := cm[name]; ok {
						if m, ok := collectorMap.(map[string]any); ok && len(m) > 0 {
							var override CollectorOverride
							if err := decodeCollectorOverride(m, &override); err != nil {
								return nil, fmt.Errorf("collector %q: %w", name, err)
							}
							cfg[name] = override
						}
					}
				}
			}
			_ = prefix // used for clarity
		}
	} else {
		for name := range knownCollectorFields {
			collectorSub := sub.Sub(name)
			if collectorSub == nil {
				continue
			}
			var override CollectorOverride
			if err := collectorSub.Unmarshal(&override); err != nil {
				return nil, fmt.Errorf("collector %q: %w", name, err)
			}
			cfg[name] = override
		}
	}

	if err := validateCollectorsConfig(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// decodeCollectorOverride manually decodes a map[string]any into CollectorOverride.
func decodeCollectorOverride(m map[string]any, o *CollectorOverride) error {
	if v, ok := m["enabled"]; ok {
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("enabled: expected bool")
		}
		o.Enabled = &b
	}
	if v, ok := m["interval_seconds"]; ok {
		i, err := toInt(v)
		if err != nil {
			return fmt.Errorf("interval_seconds: %w", err)
		}
		o.IntervalSeconds = &i
	}
	if v, ok := m["include_queries"]; ok {
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("include_queries: expected bool")
		}
		o.IncludeQueries = &b
	}
	if v, ok := m["max_query_text_length"]; ok {
		i, err := toInt(v)
		if err != nil {
			return fmt.Errorf("max_query_text_length: %w", err)
		}
		o.MaxQueryTextLength = &i
	}
	if v, ok := m["diff_limit"]; ok {
		i, err := toInt(v)
		if err != nil {
			return fmt.Errorf("diff_limit: %w", err)
		}
		o.DiffLimit = &i
	}
	if v, ok := m["include_table_data"]; ok {
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("include_table_data: expected bool")
		}
		o.IncludeTableData = &b
	}
	if v, ok := m["backfill_batch_size"]; ok {
		i, err := toInt(v)
		if err != nil {
			return fmt.Errorf("backfill_batch_size: %w", err)
		}
		o.BackfillBatchSize = &i
	}
	if v, ok := m["category_limit"]; ok {
		i, err := toInt(v)
		if err != nil {
			return fmt.Errorf("category_limit: %w", err)
		}
		o.CategoryLimit = &i
	}
	return nil
}

// toInt converts a value to int, handling common numeric types from viper/YAML.
func toInt(v any) (int, error) {
	switch val := v.(type) {
	case int:
		return val, nil
	case int64:
		return int(val), nil
	case float64:
		return int(val), nil
	default:
		return 0, fmt.Errorf("expected numeric value, got %T", v)
	}
}
