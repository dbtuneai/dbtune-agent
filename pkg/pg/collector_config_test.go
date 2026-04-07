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
	assert.Equal(t, 42, IntOr(intPtr(42), 10))
	assert.Equal(t, 10, IntOr(nil, 10))
}

func TestBoolOr(t *testing.T) {
	assert.True(t, BoolOr(boolPtr(true), false))
	assert.False(t, BoolOr(nil, false))
	assert.True(t, BoolOr(nil, true))
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
