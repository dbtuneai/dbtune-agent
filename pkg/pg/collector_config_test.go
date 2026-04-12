package pg

import (
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
		wantErr  bool
	}{
		{"nil returns default", nil, 5 * time.Second, 5 * time.Second, false},
		{"above default returns configured", intPtr(30), 5 * time.Second, 30 * time.Second, false},
		{"below default is error", intPtr(1), 5 * time.Second, 0, true},
		{"equal to default returns default", intPtr(5), 5 * time.Second, 5 * time.Second, false},
		{"zero returns default", intPtr(0), 5 * time.Second, 5 * time.Second, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := collectorconfig.BaseConfig{IntervalSeconds: tt.seconds}
			got, err := b.IntervalOr(tt.def)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
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

		assert.True(t, cfg.PgStatStatements.IsEnabled())
		assert.Equal(t, 30, *cfg.PgStatStatements.Base.IntervalSeconds)
		assert.Equal(t, 200, cfg.PgStatStatements.Extra.DiffLimit)
		assert.False(t, cfg.PgStatStatements.Extra.IncludeQueries)

		assert.False(t, cfg.PgClass.IsEnabled())
		assert.Equal(t, 250, cfg.PgClass.Extra.BackfillBatchSize)
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

		assert.True(t, cfg.PgStatStatements.IsEnabled())
		assert.Equal(t, 60, *cfg.PgStatStatements.Base.IntervalSeconds)
		assert.Equal(t, 300, cfg.PgStatStatements.Extra.DiffLimit)
		assert.True(t, cfg.PgStatStatements.Extra.IncludeQueries)
		assert.Equal(t, 2048, cfg.PgStatStatements.Extra.MaxQueryTextLength)

		assert.True(t, cfg.PgStats.Extra.IncludeTableData)
		assert.Equal(t, 150, cfg.PgStats.Extra.BackfillBatchSize)

		assert.Equal(t, 100, cfg.PgStatUserTables.Extra.CategoryLimit)
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

		assert.True(t, cfg.PgClass.IsEnabled())
		assert.Nil(t, cfg.PgClass.Base.IntervalSeconds)
		// Extra should have defaults since no extra fields were set
		assert.Equal(t, 500, cfg.PgClass.Extra.BackfillBatchSize)
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

		assert.False(t, cfg.PgClass.IsEnabled())
		assert.Equal(t, 999, cfg.PgClass.Extra.BackfillBatchSize)
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

		assert.True(t, cfg.PgStatStatements.IsEnabled())
		assert.Equal(t, 45, *cfg.PgStatStatements.Base.IntervalSeconds)
		assert.Equal(t, 250, cfg.PgStatStatements.Extra.DiffLimit)
		assert.False(t, cfg.PgStatStatements.Extra.IncludeQueries)
		assert.Equal(t, 4096, cfg.PgStatStatements.Extra.MaxQueryTextLength)

		assert.True(t, cfg.PgStats.Extra.IncludeTableData)
		assert.Equal(t, 300, cfg.PgStats.Extra.BackfillBatchSize)

		assert.Equal(t, 50, cfg.PgStatUserTables.Extra.CategoryLimit)
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

		assert.False(t, cfg.PgClass.IsEnabled())
		// YAML batch size should be preserved (env didn't override it)
		assert.Equal(t, 100, cfg.PgClass.Extra.BackfillBatchSize)
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

	t.Run("empty config has defaults", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		// Typed collectors should have their defaults from struct tags
		assert.Equal(t, queries.PgStatStatementsConfig{DiffLimit: 500, MaxQueryTextLength: 8192}, cfg.PgStatStatements.Extra)
		assert.Equal(t, queries.PgClassConfig{BackfillBatchSize: 500}, cfg.PgClass.Extra)
		assert.True(t, cfg.PgClass.IsEnabled()) // nil Enabled = enabled
	})

	t.Run("unknown env var collector rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_TOTALLY_FAKE_ENABLED", "true")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not match any known collector")
	})

	t.Run("database_average_query_runtime config round-trip", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  database_average_query_runtime:
    include_queries: true
    max_query_text_length: 2048
`))
		require.NoError(t, err)

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		assert.True(t, cfg.DatabaseAvgQueryRuntime.Extra.IncludeQueries)
		assert.Equal(t, 2048, cfg.DatabaseAvgQueryRuntime.Extra.MaxQueryTextLength)
	})

	t.Run("env var overrides extra field while preserving other YAML extra fields", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_statements:
    diff_limit: 200
    include_queries: true
    max_query_text_length: 1024
`))
		require.NoError(t, err)

		// Override only diff_limit via env; include_queries and max_query_text_length
		// should be preserved from YAML.
		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_DIFF_LIMIT", "400")

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		assert.Equal(t, 400, cfg.PgStatStatements.Extra.DiffLimit)
		assert.True(t, cfg.PgStatStatements.Extra.IncludeQueries)
		assert.Equal(t, 1024, cfg.PgStatStatements.Extra.MaxQueryTextLength)
	})

	t.Run("env var prefix ambiguity resolved by longest match", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		// DBT_COLLECTOR_PG_STAT_WAL_RECEIVER_ENABLED starts with the prefix
		// for pg_stat_wal, but should bind to pg_stat_wal_receiver.
		t.Setenv("DBT_COLLECTOR_PG_STAT_WAL_RECEIVER_ENABLED", "false")

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		walReceiver := cfg.Simple[queries.PgStatWalReceiverName]
		assert.False(t, walReceiver.IsEnabled())

		// pg_stat_wal should still have default (enabled).
		pgStatWal := cfg.Simple[queries.PgStatWalName]
		assert.True(t, pgStatWal.IsEnabled())
	})

	t.Run("simple collector with only base config", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  autovacuum_count:
    enabled: false
    interval_seconds: 30
`))
		require.NoError(t, err)

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		ac := cfg.Simple[queries.AutovacuumCountName]
		assert.False(t, ac.IsEnabled())
		assert.Equal(t, 30, *ac.IntervalSeconds)
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

	t.Run("extra field on simple collector rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  autovacuum_count:
    diff_limit: 100
`))
		require.NoError(t, err)

		_, err = CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown field")
	})
}

func TestAllTypedCollectorDefaults(t *testing.T) {
	viper.Reset()
	defer viper.Reset()

	cfg, err := CollectorsConfigFromViper()
	require.NoError(t, err)

	// PgStatStatements
	assert.Equal(t, 500, cfg.PgStatStatements.Extra.DiffLimit)
	assert.False(t, cfg.PgStatStatements.Extra.IncludeQueries)
	assert.Equal(t, 8192, cfg.PgStatStatements.Extra.MaxQueryTextLength)

	// PgClass
	assert.Equal(t, 500, cfg.PgClass.Extra.BackfillBatchSize)

	// PgStats
	assert.Equal(t, 200, cfg.PgStats.Extra.BackfillBatchSize)
	assert.False(t, cfg.PgStats.Extra.IncludeTableData)

	// PgStatUserTables
	assert.Equal(t, 200, cfg.PgStatUserTables.Extra.CategoryLimit)

	// PgStatUserIndexes
	assert.Equal(t, 200, cfg.PgStatUserIndexes.Extra.CategoryLimit)

	// PgStatioUserIndexes
	assert.Equal(t, 2000, cfg.PgStatioUserIndexes.Extra.BatchSize)

	// DatabaseAvgQueryRuntime
	assert.False(t, cfg.DatabaseAvgQueryRuntime.Extra.IncludeQueries)
	assert.Equal(t, 8192, cfg.DatabaseAvgQueryRuntime.Extra.MaxQueryTextLength)
}

func TestPgStatioUserIndexesRoundTrip(t *testing.T) {
	t.Run("YAML", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_statio_user_indexes:
    backfill_batch_size: 500
`))
		require.NoError(t, err)

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)
		assert.Equal(t, 500, cfg.PgStatioUserIndexes.Extra.BatchSize)
	})

	t.Run("env var", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_STATIO_USER_INDEXES_BACKFILL_BATCH_SIZE", "750")

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)
		assert.Equal(t, 750, cfg.PgStatioUserIndexes.Extra.BatchSize)
	})
}

func TestPgStatUserIndexesRoundTrip(t *testing.T) {
	t.Run("YAML", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		viper.SetConfigType("yaml")
		err := viper.ReadConfig(strings.NewReader(`
collectors:
  pg_stat_user_indexes:
    category_limit: 50
`))
		require.NoError(t, err)

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)
		assert.Equal(t, 50, cfg.PgStatUserIndexes.Extra.CategoryLimit)
	})

	t.Run("env var", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_STAT_USER_INDEXES_CATEGORY_LIMIT", "75")

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)
		assert.Equal(t, 75, cfg.PgStatUserIndexes.Extra.CategoryLimit)
	})
}

func TestEnvVarValidationConstraints(t *testing.T) {
	t.Run("env var exceeding max rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_DIFF_LIMIT", "501")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "diff_limit")
	})

	t.Run("env var below min rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_CLASS_BACKFILL_BATCH_SIZE", "-1")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backfill_batch_size")
	})

	t.Run("env var max_query_text_length exceeding max rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_PG_STAT_STATEMENTS_MAX_QUERY_TEXT_LENGTH", "8193")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max_query_text_length")
	})
}

func TestDatabaseAvgQueryRuntimeEnvVar(t *testing.T) {
	t.Run("env var overlay", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_DATABASE_AVERAGE_QUERY_RUNTIME_INCLUDE_QUERIES", "true")
		t.Setenv("DBT_COLLECTOR_DATABASE_AVERAGE_QUERY_RUNTIME_MAX_QUERY_TEXT_LENGTH", "4096")

		cfg, err := CollectorsConfigFromViper()
		require.NoError(t, err)

		assert.True(t, cfg.DatabaseAvgQueryRuntime.Extra.IncludeQueries)
		assert.Equal(t, 4096, cfg.DatabaseAvgQueryRuntime.Extra.MaxQueryTextLength)
	})

	t.Run("env var exceeding max rejected", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_COLLECTOR_DATABASE_AVERAGE_QUERY_RUNTIME_MAX_QUERY_TEXT_LENGTH", "8193")

		_, err := CollectorsConfigFromViper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max_query_text_length")
	})
}
