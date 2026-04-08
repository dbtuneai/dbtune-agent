package pg

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/mitchellh/mapstructure"
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
		if err := mapstructure.Decode(m, &override); err != nil {
			return nil, fmt.Errorf("collector %q: %w", name, err)
		}
		cfg[name] = override
	}

	// Collect all DBT_COLLECTOR_ env vars in one pass so we only scan
	// the environment once, then do map lookups instead of per-field
	// os.Getenv calls across all ~33 known collectors.
	collectorEnvs := make(map[string]string)
	for _, env := range os.Environ() {
		if k, v, ok := strings.Cut(env, "="); ok && strings.HasPrefix(k, "DBT_COLLECTOR_") {
			collectorEnvs[k] = v
		}
	}

	if len(collectorEnvs) > 0 {
		for name, extraFields := range knownCollectorFields {
			envPrefix := "DBT_COLLECTOR_" + strings.ToUpper(name) + "_"
			override := cfg[name]
			changed := false

			// Universal fields
			if v, ok := collectorEnvs[envPrefix+"ENABLED"]; ok {
				b, err := strconv.ParseBool(v)
				if err != nil {
					return nil, fmt.Errorf("env %sENABLED: %w", envPrefix, err)
				}
				override.Enabled = &b
				changed = true
			}
			if v, ok := collectorEnvs[envPrefix+"INTERVAL_SECONDS"]; ok {
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
				v, ok := collectorEnvs[envKey]
				if !ok {
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
	}

	if err := validateCollectorsConfig(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
