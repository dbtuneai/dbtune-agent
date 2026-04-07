package pg

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

// CollectorsConfigFromViper reads the collectors YAML section, overlays
// environment variables with pattern DBT_COLLECTOR_{NAME}_{FIELD},
// validates, and returns the config.
func CollectorsConfigFromViper() (CollectorsConfig, error) {
	cfg := make(CollectorsConfig)

	// Read from YAML collectors section
	rawMap := viper.GetStringMap("collectors")
	for name, v := range rawMap {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("collector %q: expected map, got %T", name, v)
		}
		var override CollectorOverride
		if err := decodeCollectorOverride(m, &override); err != nil {
			return nil, fmt.Errorf("collector %q: %w", name, err)
		}
		cfg[name] = override
	}

	// Overlay environment variables for known collectors
	for name, extraFields := range knownCollectorFields {
		envPrefix := "DBT_COLLECTOR_" + strings.ToUpper(name) + "_"
		override := cfg[name]
		changed := false

		// Universal fields
		if v := os.Getenv(envPrefix + "ENABLED"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("env %sENABLED: %w", envPrefix, err)
			}
			override.Enabled = &b
			changed = true
		}
		if v := os.Getenv(envPrefix + "INTERVAL_SECONDS"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("env %sINTERVAL_SECONDS: %w", envPrefix, err)
			}
			override.IntervalSeconds = &n
			changed = true
		}

		// Collector-specific extra fields
		for _, field := range extraFields {
			envKey := envPrefix + strings.ToUpper(field)
			v := os.Getenv(envKey)
			if v == "" {
				continue
			}
			var err error
			switch field {
			case "include_queries":
				var b bool
				b, err = strconv.ParseBool(v)
				if err == nil {
					override.IncludeQueries = &b
				}
			case "max_query_text_length":
				var n int
				n, err = strconv.Atoi(v)
				if err == nil {
					override.MaxQueryTextLength = &n
				}
			case "diff_limit":
				var n int
				n, err = strconv.Atoi(v)
				if err == nil {
					override.DiffLimit = &n
				}
			case "include_table_data":
				var b bool
				b, err = strconv.ParseBool(v)
				if err == nil {
					override.IncludeTableData = &b
				}
			case "backfill_batch_size":
				var n int
				n, err = strconv.Atoi(v)
				if err == nil {
					override.BackfillBatchSize = &n
				}
			case "category_limit":
				var n int
				n, err = strconv.Atoi(v)
				if err == nil {
					override.CategoryLimit = &n
				}
			}
			if err != nil {
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

// decodeCollectorOverride manually decodes a map[string]any into a CollectorOverride.
func decodeCollectorOverride(m map[string]any, o *CollectorOverride) error {
	if v, ok := m["enabled"]; ok {
		b, err := toBool(v)
		if err != nil {
			return fmt.Errorf("enabled: %w", err)
		}
		o.Enabled = &b
	}
	if v, ok := m["interval_seconds"]; ok {
		n, err := toInt(v)
		if err != nil {
			return fmt.Errorf("interval_seconds: %w", err)
		}
		o.IntervalSeconds = &n
	}
	if v, ok := m["include_queries"]; ok {
		b, err := toBool(v)
		if err != nil {
			return fmt.Errorf("include_queries: %w", err)
		}
		o.IncludeQueries = &b
	}
	if v, ok := m["max_query_text_length"]; ok {
		n, err := toInt(v)
		if err != nil {
			return fmt.Errorf("max_query_text_length: %w", err)
		}
		o.MaxQueryTextLength = &n
	}
	if v, ok := m["diff_limit"]; ok {
		n, err := toInt(v)
		if err != nil {
			return fmt.Errorf("diff_limit: %w", err)
		}
		o.DiffLimit = &n
	}
	if v, ok := m["include_table_data"]; ok {
		b, err := toBool(v)
		if err != nil {
			return fmt.Errorf("include_table_data: %w", err)
		}
		o.IncludeTableData = &b
	}
	if v, ok := m["backfill_batch_size"]; ok {
		n, err := toInt(v)
		if err != nil {
			return fmt.Errorf("backfill_batch_size: %w", err)
		}
		o.BackfillBatchSize = &n
	}
	if v, ok := m["category_limit"]; ok {
		n, err := toInt(v)
		if err != nil {
			return fmt.Errorf("category_limit: %w", err)
		}
		o.CategoryLimit = &n
	}
	return nil
}

// toInt converts a value from YAML/viper to int, handling int, int64, float64.
func toInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		return int(n), nil
	default:
		return 0, fmt.Errorf("expected number, got %T", v)
	}
}

// toBool converts a value from YAML/viper to bool.
func toBool(v any) (bool, error) {
	switch b := v.(type) {
	case bool:
		return b, nil
	default:
		return false, fmt.Errorf("expected bool, got %T", v)
	}
}
