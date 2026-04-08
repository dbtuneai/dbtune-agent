package pg

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }
func intPtr(n int) *int    { return &n }

func TestCollectorOverride_IsEnabled(t *testing.T) {
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
			o := CollectorOverride{Enabled: tt.enabled}
			assert.Equal(t, tt.expected, o.IsEnabled())
		})
	}
}

func TestCollectorOverride_IntervalOr(t *testing.T) {
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
			o := CollectorOverride{IntervalSeconds: tt.seconds}
			assert.Equal(t, tt.expected, o.IntervalOr(tt.def))
		})
	}
}

func TestIntOr(t *testing.T) {
	assert.Equal(t, 42, intOr(intPtr(42), 10))
	assert.Equal(t, 10, intOr(nil, 10))
}

func TestBoolOr(t *testing.T) {
	assert.True(t, boolOr(boolPtr(true), false))
	assert.False(t, boolOr(nil, false))
	assert.True(t, boolOr(nil, true))
}

func TestValidateCollectorsConfig(t *testing.T) {
	t.Run("unknown collector name", func(t *testing.T) {
		cfg := CollectorsConfig{"not_a_real_collector": {}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown collector")
	})

	t.Run("negative interval_seconds", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_class": {IntervalSeconds: intPtr(-1)}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "interval_seconds")
	})

	t.Run("diff_limit exceeds max", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_stat_statements": {DiffLimit: intPtr(MaxDiffLimit + 1)}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "diff_limit")
	})

	t.Run("negative diff_limit", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_stat_statements": {DiffLimit: intPtr(-1)}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "diff_limit")
	})

	t.Run("diff_limit on wrong collector", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_class": {DiffLimit: intPtr(100)}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid field")
	})

	t.Run("max_query_text_length exceeds max", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_stat_statements": {MaxQueryTextLength: intPtr(MaxQueryTextLength + 1)}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max_query_text_length")
	})

	t.Run("backfill_batch_size negative", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_class": {BackfillBatchSize: intPtr(-1)}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backfill_batch_size")
	})

	t.Run("backfill_batch_size on wrong collector", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_database": {BackfillBatchSize: intPtr(100)}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid field")
	})

	t.Run("category_limit negative", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_stat_user_tables": {CategoryLimit: intPtr(-1)}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "category_limit")
	})

	t.Run("include_queries on wrong collector", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_class": {IncludeQueries: boolPtr(true)}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid field")
	})

	t.Run("include_table_data on wrong collector", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_class": {IncludeTableData: boolPtr(true)}}
		err := validateCollectorsConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid field")
	})

	t.Run("valid config passes", func(t *testing.T) {
		cfg := CollectorsConfig{
			"pg_stat_statements":  {DiffLimit: intPtr(200), IncludeQueries: boolPtr(true)},
			"pg_class":            {BackfillBatchSize: intPtr(250)},
			"pg_stats":            {IncludeTableData: boolPtr(false), BackfillBatchSize: intPtr(100)},
			"pg_stat_user_tables": {CategoryLimit: intPtr(150)},
		}
		err := validateCollectorsConfig(cfg)
		require.NoError(t, err)
	})

	t.Run("empty config passes", func(t *testing.T) {
		err := validateCollectorsConfig(CollectorsConfig{})
		require.NoError(t, err)
	})
}

func TestValidateCollectorsConfig_BoundaryValues(t *testing.T) {
	t.Run("diff_limit at zero", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_stat_statements": {DiffLimit: intPtr(0)}}
		require.NoError(t, validateCollectorsConfig(cfg))
	})

	t.Run("diff_limit at max", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_stat_statements": {DiffLimit: intPtr(MaxDiffLimit)}}
		require.NoError(t, validateCollectorsConfig(cfg))
	})

	t.Run("max_query_text_length at zero", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_stat_statements": {MaxQueryTextLength: intPtr(0)}}
		require.NoError(t, validateCollectorsConfig(cfg))
	})

	t.Run("max_query_text_length at max", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_stat_statements": {MaxQueryTextLength: intPtr(MaxQueryTextLength)}}
		require.NoError(t, validateCollectorsConfig(cfg))
	})

	t.Run("interval_seconds at zero", func(t *testing.T) {
		cfg := CollectorsConfig{"pg_class": {IntervalSeconds: intPtr(0)}}
		require.NoError(t, validateCollectorsConfig(cfg))
	})
}

func TestValidateCollectorsConfig_MultipleErrors(t *testing.T) {
	cfg := CollectorsConfig{
		"not_real":            {},
		"pg_stat_statements":  {DiffLimit: intPtr(-1)},
		"pg_stat_user_tables": {CategoryLimit: intPtr(-1)},
	}
	err := validateCollectorsConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown collector")
	assert.Contains(t, err.Error(), "diff_limit")
	assert.Contains(t, err.Error(), "category_limit")
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
		assert.Equal(t, 30, *pss.IntervalSeconds)
		assert.Equal(t, 200, *pss.DiffLimit)
		assert.False(t, *pss.IncludeQueries)

		pc := cfg["pg_class"]
		assert.False(t, pc.IsEnabled())
		assert.Equal(t, 250, *pc.BackfillBatchSize)
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
		assert.Equal(t, 60, *pss.IntervalSeconds)
		assert.Equal(t, 300, *pss.DiffLimit)
		assert.True(t, *pss.IncludeQueries)
		assert.Equal(t, 2048, *pss.MaxQueryTextLength)

		ps := cfg["pg_stats"]
		assert.True(t, *ps.IncludeTableData)
		assert.Equal(t, 150, *ps.BackfillBatchSize)

		psut := cfg["pg_stat_user_tables"]
		assert.Equal(t, 100, *psut.CategoryLimit)
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
		assert.Nil(t, pc.IntervalSeconds)
		assert.Nil(t, pc.BackfillBatchSize)
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

	t.Run("env var overlay", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_CLASS_ENABLED", "false")
		t.Setenv("DBT_COLLECTOR_PG_CLASS_BACKFILL_BATCH_SIZE", "999")

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		pc := cfg["pg_class"]
		assert.False(t, pc.IsEnabled())
		assert.Equal(t, 999, *pc.BackfillBatchSize)
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
		assert.Equal(t, 45, *pss.IntervalSeconds)
		assert.Equal(t, 250, *pss.DiffLimit)
		assert.False(t, *pss.IncludeQueries)
		assert.Equal(t, 4096, *pss.MaxQueryTextLength)

		ps := cfg["pg_stats"]
		assert.True(t, *ps.IncludeTableData)
		assert.Equal(t, 300, *ps.BackfillBatchSize)

		psut := cfg["pg_stat_user_tables"]
		assert.Equal(t, 50, *psut.CategoryLimit)
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
		assert.Equal(t, 100, *pc.BackfillBatchSize)
	})

	t.Run("invalid env var bool rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_CLASS_ENABLED", "notabool")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ENABLED")
	})

	t.Run("invalid env var int rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_CLASS_INTERVAL_SECONDS", "notanumber")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "INTERVAL_SECONDS")
	})

	t.Run("invalid env var extra field int rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_DIFF_LIMIT", "xyz")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DIFF_LIMIT")
	})

	t.Run("invalid env var extra field bool rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_INCLUDE_QUERIES", "notabool")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "INCLUDE_QUERIES")
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
