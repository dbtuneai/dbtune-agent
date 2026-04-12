package pg

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }
func intPtr(n int) *int    { return &n }

func TestBaseConfig_IsEnabled(t *testing.T) {
	tests := []struct {
		name     string
		enabled  *bool
		expected bool
	}{
		{"nil returns true", nil, true},
		{"true returns true", boolPtr(true), true},
		{"false returns false", boolPtr(false), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := collectorconfig.BaseConfig{Enabled: tt.enabled}
			assert.Equal(t, tt.expected, b.IsEnabled())
		})
	}
}

func TestBaseConfig_IntervalOr(t *testing.T) {
	tests := []struct {
		name     string
		seconds  *int
		def      time.Duration
		expected time.Duration
	}{
		{"nil returns default", nil, 5 * time.Second, 5 * time.Second},
		{"above default returns configured", intPtr(30), 5 * time.Second, 30 * time.Second},
		{"below default is clamped", intPtr(1), 5 * time.Second, 5 * time.Second},
		{"equal to default returns default", intPtr(5), 5 * time.Second, 5 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := collectorconfig.BaseConfig{IntervalSeconds: tt.seconds}
			assert.Equal(t, tt.expected, b.IntervalOr(tt.def))
		})
	}
}

func TestCollectorsConfigFromViper(t *testing.T) {
	t.Run("round-trip YAML config", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_statements:
    enabled: true
    interval_seconds: 30
    diff_limit: 200
    include_queries: false
  pg_class:
    enabled: false
    backfill_batch_size: 250
`))
		require.NoError(t, err)

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		pss := cfg["pg_stat_statements"]
		assert.True(t, pss.IsEnabled())
		assert.Equal(t, 30, *pss.Base.IntervalSeconds)
		pssCfg := pss.Extra.(queries.PgStatStatementsConfig)
		assert.Equal(t, 200, pssCfg.DiffLimit)
		assert.False(t, pssCfg.IncludeQueries)

		pc := cfg["pg_class"]
		assert.False(t, pc.IsEnabled())
		pcCfg := pc.Extra.(queries.PgClassConfig)
		assert.Equal(t, 250, pcCfg.BackfillBatchSize)
	})

	t.Run("round-trip all fields via YAML", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_statements:
    enabled: true
    interval_seconds: 60
    diff_limit: 300
    include_queries: true
    max_query_text_length: 2048
  pg_stats:
    include_table_data: true
    backfill_batch_size: 150
  pg_stat_user_tables:
    category_limit: 100
`))
		require.NoError(t, err)

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		pss := cfg["pg_stat_statements"]
		assert.True(t, pss.IsEnabled())
		assert.Equal(t, 60, *pss.Base.IntervalSeconds)
		pssCfg := pss.Extra.(queries.PgStatStatementsConfig)
		assert.Equal(t, 300, pssCfg.DiffLimit)
		assert.True(t, pssCfg.IncludeQueries)
		assert.Equal(t, 2048, pssCfg.MaxQueryTextLength)

		ps := cfg["pg_stats"]
		psCfg := ps.Extra.(queries.PgStatsConfig)
		assert.True(t, psCfg.IncludeTableData)
		assert.Equal(t, 150, psCfg.BackfillBatchSize)

		psut := cfg["pg_stat_user_tables"]
		psutCfg := psut.Extra.(queries.PgStatUserTablesConfig)
		assert.Equal(t, 100, psutCfg.CategoryLimit)
	})

	t.Run("minimal config with only enabled", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_class:
    enabled: true
`))
		require.NoError(t, err)

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		pc := cfg["pg_class"]
		assert.True(t, pc.IsEnabled())
		assert.Nil(t, pc.Base.IntervalSeconds)
		assert.Nil(t, pc.Extra) // no extra fields set
	})

	t.Run("non-map collector value rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_class: true
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected map")
	})

	t.Run("unknown YAML field rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_class:
    not_a_real_field: 123
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown field")
	})

	t.Run("env var overlay", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_CLASS_ENABLED", "false")
		t.Setenv("DBT_COLLECTOR_PG_CLASS_BACKFILL_BATCH_SIZE", "999")

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		pc := cfg["pg_class"]
		assert.False(t, pc.IsEnabled())
		pcCfg := pc.Extra.(queries.PgClassConfig)
		assert.Equal(t, 999, pcCfg.BackfillBatchSize)
	})

	t.Run("env var overlay for all field types", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_ENABLED", "true")
		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_INTERVAL_SECONDS", "45")
		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_DIFF_LIMIT", "250")
		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_INCLUDE_QUERIES", "false")
		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_MAX_QUERY_TEXT_LENGTH", "4096")
		t.Setenv("DBT_COLLECTOR_PG_STATS_INCLUDE_TABLE_DATA", "true")
		t.Setenv("DBT_COLLECTOR_PG_STATS_BACKFILL_BATCH_SIZE", "300")
		t.Setenv("DBT_COLLECTOR_PG_STAT_USER_TABLES_CATEGORY_LIMIT", "50")

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		pss := cfg["pg_stat_statements"]
		assert.True(t, pss.IsEnabled())
		assert.Equal(t, 45, *pss.Base.IntervalSeconds)
		pssCfg := pss.Extra.(queries.PgStatStatementsConfig)
		assert.Equal(t, 250, pssCfg.DiffLimit)
		assert.False(t, pssCfg.IncludeQueries)
		assert.Equal(t, 4096, pssCfg.MaxQueryTextLength)

		ps := cfg["pg_stats"]
		psCfg := ps.Extra.(queries.PgStatsConfig)
		assert.True(t, psCfg.IncludeTableData)
		assert.Equal(t, 300, psCfg.BackfillBatchSize)

		psut := cfg["pg_stat_user_tables"]
		psutCfg := psut.Extra.(queries.PgStatUserTablesConfig)
		assert.Equal(t, 50, psutCfg.CategoryLimit)
	})

	t.Run("env var overrides YAML", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_class:
    enabled: true
    backfill_batch_size: 100
`))
		require.NoError(t, err)

		t.Setenv("DBT_COLLECTOR_PG_CLASS_ENABLED", "false")

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		pc := cfg["pg_class"]
		assert.False(t, pc.IsEnabled())
		// YAML batch size should be preserved (env didn't override it)
		pcCfg := pc.Extra.(queries.PgClassConfig)
		assert.Equal(t, 100, pcCfg.BackfillBatchSize)
	})

	t.Run("invalid env var bool rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_CLASS_ENABLED", "notabool")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "enabled")
	})

	t.Run("invalid env var int rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_CLASS_INTERVAL_SECONDS", "notanumber")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "interval_seconds")
	})

	t.Run("invalid env var extra field int rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_DIFF_LIMIT", "xyz")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "diff_limit")
	})

	t.Run("invalid env var extra field bool rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_INCLUDE_QUERIES", "notabool")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "include_queries")
	})

	t.Run("unknown collector rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  unknown_collector:
    enabled: true
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown collector")
	})

	t.Run("empty config returns empty map", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)
		assert.Empty(t, cfg)
	})
}

func TestValidation(t *testing.T) {
	t.Run("diff_limit exceeds max rejected via YAML", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_statements:
    diff_limit: 501
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "diff_limit")
	})

	t.Run("negative diff_limit rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_statements:
    diff_limit: -1
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "diff_limit")
	})

	t.Run("diff_limit on wrong collector rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_class:
    diff_limit: 100
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown field")
	})

	t.Run("max_query_text_length exceeds max rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_statements:
    max_query_text_length: 8193
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max_query_text_length")
	})

	t.Run("negative backfill_batch_size rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_class:
    backfill_batch_size: -1
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backfill_batch_size")
	})

	t.Run("backfill_batch_size on wrong collector rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_database:
    backfill_batch_size: 100
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown field")
	})

	t.Run("negative category_limit rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_user_tables:
    category_limit: -1
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "category_limit")
	})

	t.Run("include_queries on wrong collector rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_class:
    include_queries: true
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown field")
	})

	t.Run("include_table_data on wrong collector rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_class:
    include_table_data: true
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown field")
	})

	t.Run("valid config passes", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_statements:
    diff_limit: 200
    include_queries: true
  pg_class:
    backfill_batch_size: 250
  pg_stats:
    include_table_data: false
    backfill_batch_size: 100
  pg_stat_user_tables:
    category_limit: 150
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.NoError(t, err)
	})

	t.Run("boundary: diff_limit at zero", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_statements:
    diff_limit: 0
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.NoError(t, err)
	})

	t.Run("boundary: diff_limit at max", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_statements:
    diff_limit: 500
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.NoError(t, err)
	})

	t.Run("negative interval_seconds rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_class:
    interval_seconds: -1
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "interval_seconds")
	})

	t.Run("boundary: interval_seconds at zero", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_class:
    interval_seconds: 0
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.NoError(t, err)
	})
}

func TestCatalogRegistrations_AllMarkedAsCatalogKind(t *testing.T) {
	for _, reg := range queries.CatalogRegistrations() {
		assert.True(t, reg.Kind.Has(collectorconfig.CatalogCollectorKind),
			"collector %q should include catalog kind", reg.Name)
	}
}

func TestKnownCollectors_MetricCollectorKeysMatchAdapters(t *testing.T) {
	actualMetricNames := collectMetricCollectorKeysFromSource(t)
	registry := allRegistrations()

	configuredMetricNames := make(map[string]struct{})
	for name, reg := range registry {
		if !reg.Kind.Has(collectorconfig.MetricCollectorKind) {
			continue
		}
		configuredMetricNames[name] = struct{}{}
	}

	assert.Equal(t, actualMetricNames, configuredMetricNames)
}

func collectMetricCollectorKeysFromSource(t *testing.T) map[string]struct{} {
	t.Helper()

	files := []string{
		"../aiven/adapter.go",
		"../azureflex/adapter.go",
		"../cloudsql/adapter.go",
		"../cnpg/adapter.go",
		"../docker/adapter.go",
		"../patroni/adapter.go",
		"../pgprem/adapter.go",
		"../rds/adapters.go",
	}

	keys := make(map[string]struct{})
	for _, relPath := range files {
		path := filepath.Clean(relPath)
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		require.NoError(t, err, "parse %s", path)

		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}

			for _, elt := range lit.Elts {
				keyValue, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				ident, ok := keyValue.Key.(*ast.Ident)
				if !ok || ident.Name != "Key" {
					continue
				}
				value, ok := keyValue.Value.(*ast.BasicLit)
				if !ok || value.Kind != token.STRING {
					continue
				}
				keys[strings.Trim(value.Value, `"`)] = struct{}{}
			}
			return true
		})
	}

	return keys
}
