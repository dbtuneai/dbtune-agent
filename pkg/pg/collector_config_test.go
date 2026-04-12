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

func TestAllTypedCollectorDefaults(t *testing.T) {
	viper.Reset()
	defer viper.Reset()

	cfg, err := CollectorsConfigFromViper()
	require.NoError(t, err)

	assert.Equal(t, 500, cfg.PgStatStatements.Extra.DiffLimit)
	assert.False(t, cfg.PgStatStatements.Extra.IncludeQueries)
	assert.Equal(t, 8192, cfg.PgStatStatements.Extra.MaxQueryTextLength)

	assert.Equal(t, 500, cfg.PgClass.Extra.BackfillBatchSize)

	assert.Equal(t, 200, cfg.PgStats.Extra.BackfillBatchSize)
	assert.False(t, cfg.PgStats.Extra.IncludeTableData)

	assert.Equal(t, 200, cfg.PgStatUserTables.Extra.CategoryLimit)

	assert.Equal(t, 200, cfg.PgStatUserIndexes.Extra.CategoryLimit)

	assert.Equal(t, 2000, cfg.PgStatioUserIndexes.Extra.BatchSize)

	assert.False(t, cfg.DatabaseAvgQueryRuntime.Extra.IncludeQueries)
	assert.Equal(t, 8192, cfg.DatabaseAvgQueryRuntime.Extra.MaxQueryTextLength)
}

type configTestCase struct {
	name            string
	yaml            string
	envVars         map[string]string
	wantErrContains string
	check           func(t *testing.T, cfg CollectorsConfig)
}

func runConfigTests(t *testing.T, tests []configTestCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			defer viper.Reset()

			if tt.yaml != "" {
				viper.SetConfigType("yaml")
				require.NoError(t, viper.ReadConfig(strings.NewReader(tt.yaml)))
			}
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			cfg, err := CollectorsConfigFromViper()
			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func TestCollectorsConfigFromViper(t *testing.T) {
	runConfigTests(t, []configTestCase{
		// --- YAML round-trips ---
		{
			name: "round-trip YAML config",
			yaml: `
collectors:
  pg_stat_statements:
    enabled: true
    interval_seconds: 30
    diff_limit: 200
    include_queries: false
  pg_class:
    enabled: false
    backfill_batch_size: 250`,
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.True(t, cfg.PgStatStatements.Base.IsEnabled())
				assert.Equal(t, 30, *cfg.PgStatStatements.Base.IntervalSeconds)
				assert.Equal(t, 200, cfg.PgStatStatements.Extra.DiffLimit)
				assert.False(t, cfg.PgStatStatements.Extra.IncludeQueries)
				assert.False(t, cfg.PgClass.Base.IsEnabled())
				assert.Equal(t, 250, cfg.PgClass.Extra.BackfillBatchSize)
			},
		},
		{
			name: "round-trip all fields via YAML",
			yaml: `
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
    category_limit: 100`,
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.True(t, cfg.PgStatStatements.Base.IsEnabled())
				assert.Equal(t, 60, *cfg.PgStatStatements.Base.IntervalSeconds)
				assert.Equal(t, 300, cfg.PgStatStatements.Extra.DiffLimit)
				assert.True(t, cfg.PgStatStatements.Extra.IncludeQueries)
				assert.Equal(t, 2048, cfg.PgStatStatements.Extra.MaxQueryTextLength)
				assert.True(t, cfg.PgStats.Extra.IncludeTableData)
				assert.Equal(t, 150, cfg.PgStats.Extra.BackfillBatchSize)
				assert.Equal(t, 100, cfg.PgStatUserTables.Extra.CategoryLimit)
			},
		},
		{
			name: "minimal config with only enabled",
			yaml: `
collectors:
  pg_class:
    enabled: true`,
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.True(t, cfg.PgClass.Base.IsEnabled())
				assert.Nil(t, cfg.PgClass.Base.IntervalSeconds)
				assert.Equal(t, 500, cfg.PgClass.Extra.BackfillBatchSize)
			},
		},
		{
			name: "database_average_query_runtime YAML",
			yaml: `
collectors:
  database_average_query_runtime:
    include_queries: true
    max_query_text_length: 2048`,
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.True(t, cfg.DatabaseAvgQueryRuntime.Extra.IncludeQueries)
				assert.Equal(t, 2048, cfg.DatabaseAvgQueryRuntime.Extra.MaxQueryTextLength)
			},
		},
		{
			name: "pg_statio_user_indexes YAML",
			yaml: `
collectors:
  pg_statio_user_indexes:
    backfill_batch_size: 500`,
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.Equal(t, 500, cfg.PgStatioUserIndexes.Extra.BatchSize)
			},
		},
		{
			name: "pg_stat_user_indexes YAML",
			yaml: `
collectors:
  pg_stat_user_indexes:
    category_limit: 50`,
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.Equal(t, 50, cfg.PgStatUserIndexes.Extra.CategoryLimit)
			},
		},
		{
			name: "simple collector with only base config",
			yaml: `
collectors:
  autovacuum_count:
    enabled: false
    interval_seconds: 30`,
			check: func(t *testing.T, cfg CollectorsConfig) {
				ac := cfg.Simple[queries.AutovacuumCountName]
				assert.False(t, ac.IsEnabled())
				assert.Equal(t, 30, *ac.IntervalSeconds)
			},
		},
		{
			name: "empty config has defaults",
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.Equal(t, queries.PgStatStatementsConfig{DiffLimit: 500, MaxQueryTextLength: 8192}, cfg.PgStatStatements.Extra)
				assert.Equal(t, queries.PgClassConfig{BackfillBatchSize: 500}, cfg.PgClass.Extra)
				assert.True(t, cfg.PgClass.Base.IsEnabled())
			},
		},

		// --- Env var overlays ---
		{
			name:    "env var overlay",
			envVars: map[string]string{"DBT_COLLECTOR_PG_CLASS_ENABLED": "false", "DBT_COLLECTOR_PG_CLASS_BACKFILL_BATCH_SIZE": "999"},
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.False(t, cfg.PgClass.Base.IsEnabled())
				assert.Equal(t, 999, cfg.PgClass.Extra.BackfillBatchSize)
			},
		},
		{
			name: "env var overlay for all field types",
			envVars: map[string]string{
				"DBT_COLLECTOR_PG_STAT_STATEMENTS_ENABLED":               "true",
				"DBT_COLLECTOR_PG_STAT_STATEMENTS_INTERVAL_SECONDS":      "45",
				"DBT_COLLECTOR_PG_STAT_STATEMENTS_DIFF_LIMIT":            "250",
				"DBT_COLLECTOR_PG_STAT_STATEMENTS_INCLUDE_QUERIES":       "false",
				"DBT_COLLECTOR_PG_STAT_STATEMENTS_MAX_QUERY_TEXT_LENGTH": "4096",
				"DBT_COLLECTOR_PG_STATS_INCLUDE_TABLE_DATA":              "true",
				"DBT_COLLECTOR_PG_STATS_BACKFILL_BATCH_SIZE":             "300",
				"DBT_COLLECTOR_PG_STAT_USER_TABLES_CATEGORY_LIMIT":       "50",
			},
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.True(t, cfg.PgStatStatements.Base.IsEnabled())
				assert.Equal(t, 45, *cfg.PgStatStatements.Base.IntervalSeconds)
				assert.Equal(t, 250, cfg.PgStatStatements.Extra.DiffLimit)
				assert.False(t, cfg.PgStatStatements.Extra.IncludeQueries)
				assert.Equal(t, 4096, cfg.PgStatStatements.Extra.MaxQueryTextLength)
				assert.True(t, cfg.PgStats.Extra.IncludeTableData)
				assert.Equal(t, 300, cfg.PgStats.Extra.BackfillBatchSize)
				assert.Equal(t, 50, cfg.PgStatUserTables.Extra.CategoryLimit)
			},
		},
		{
			name: "env var overrides YAML",
			yaml: `
collectors:
  pg_class:
    enabled: true
    backfill_batch_size: 100`,
			envVars: map[string]string{"DBT_COLLECTOR_PG_CLASS_ENABLED": "false"},
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.False(t, cfg.PgClass.Base.IsEnabled())
				assert.Equal(t, 100, cfg.PgClass.Extra.BackfillBatchSize)
			},
		},
		{
			name: "env var overrides extra field while preserving other YAML extra fields",
			yaml: `
collectors:
  pg_stat_statements:
    diff_limit: 200
    include_queries: true
    max_query_text_length: 1024`,
			envVars: map[string]string{"DBT_COLLECTOR_PG_STAT_STATEMENTS_DIFF_LIMIT": "400"},
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.Equal(t, 400, cfg.PgStatStatements.Extra.DiffLimit)
				assert.True(t, cfg.PgStatStatements.Extra.IncludeQueries)
				assert.Equal(t, 1024, cfg.PgStatStatements.Extra.MaxQueryTextLength)
			},
		},
		{
			name:    "env var prefix ambiguity resolved by longest match",
			envVars: map[string]string{"DBT_COLLECTOR_PG_STAT_WAL_RECEIVER_ENABLED": "false"},
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.False(t, cfg.Simple[queries.PgStatWalReceiverName].IsEnabled())
				assert.True(t, cfg.Simple[queries.PgStatWalName].IsEnabled())
			},
		},
		{
			name:    "pg_statio_user_indexes env var",
			envVars: map[string]string{"DBT_COLLECTOR_PG_STATIO_USER_INDEXES_BACKFILL_BATCH_SIZE": "750"},
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.Equal(t, 750, cfg.PgStatioUserIndexes.Extra.BatchSize)
			},
		},
		{
			name:    "pg_stat_user_indexes env var",
			envVars: map[string]string{"DBT_COLLECTOR_PG_STAT_USER_INDEXES_CATEGORY_LIMIT": "75"},
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.Equal(t, 75, cfg.PgStatUserIndexes.Extra.CategoryLimit)
			},
		},
		{
			name:    "database_average_query_runtime env var",
			envVars: map[string]string{"DBT_COLLECTOR_DATABASE_AVERAGE_QUERY_RUNTIME_INCLUDE_QUERIES": "true", "DBT_COLLECTOR_DATABASE_AVERAGE_QUERY_RUNTIME_MAX_QUERY_TEXT_LENGTH": "4096"},
			check: func(t *testing.T, cfg CollectorsConfig) {
				assert.True(t, cfg.DatabaseAvgQueryRuntime.Extra.IncludeQueries)
				assert.Equal(t, 4096, cfg.DatabaseAvgQueryRuntime.Extra.MaxQueryTextLength)
			},
		},
		{
			name: "valid multi-collector config passes",
			yaml: `
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
    category_limit: 150`,
		},

		// --- Error cases ---
		{
			name:            "non-map collector value rejected",
			yaml:            "collectors:\n  pg_class: true",
			wantErrContains: "expected map",
		},
		{
			name:            "unknown YAML field rejected",
			yaml:            "collectors:\n  pg_class:\n    not_a_real_field: 123",
			wantErrContains: "unknown field",
		},
		{
			name:            "unknown collector rejected",
			yaml:            "collectors:\n  unknown_collector:\n    enabled: true",
			wantErrContains: "unknown collector",
		},
		{
			name:            "unknown env var collector rejected",
			envVars:         map[string]string{"DBT_COLLECTOR_TOTALLY_FAKE_ENABLED": "true"},
			wantErrContains: "does not match any known collector",
		},
		{
			name:            "invalid env var bool rejected",
			envVars:         map[string]string{"DBT_COLLECTOR_PG_CLASS_ENABLED": "notabool"},
			wantErrContains: "enabled",
		},
		{
			name:            "invalid env var int rejected",
			envVars:         map[string]string{"DBT_COLLECTOR_PG_CLASS_INTERVAL_SECONDS": "notanumber"},
			wantErrContains: "interval_seconds",
		},
		{
			name:            "invalid env var extra field int rejected",
			envVars:         map[string]string{"DBT_COLLECTOR_PG_STAT_STATEMENTS_DIFF_LIMIT": "xyz"},
			wantErrContains: "diff_limit",
		},
		{
			name:            "invalid env var extra field bool rejected",
			envVars:         map[string]string{"DBT_COLLECTOR_PG_STAT_STATEMENTS_INCLUDE_QUERIES": "notabool"},
			wantErrContains: "include_queries",
		},
		{
			name:            "extra field on simple collector rejected",
			yaml:            "collectors:\n  autovacuum_count:\n    diff_limit: 100",
			wantErrContains: "unknown field",
		},

		// --- Field on wrong collector ---
		{
			name:            "diff_limit on wrong collector rejected",
			yaml:            "collectors:\n  pg_class:\n    diff_limit: 100",
			wantErrContains: "unknown field",
		},
		{
			name:            "backfill_batch_size on wrong collector rejected",
			yaml:            "collectors:\n  pg_database:\n    backfill_batch_size: 100",
			wantErrContains: "unknown field",
		},
		{
			name:            "include_queries on wrong collector rejected",
			yaml:            "collectors:\n  pg_class:\n    include_queries: true",
			wantErrContains: "unknown field",
		},
		{
			name:            "include_table_data on wrong collector rejected",
			yaml:            "collectors:\n  pg_class:\n    include_table_data: true",
			wantErrContains: "unknown field",
		},

		// --- Validation (min/max) ---
		{
			name:            "diff_limit exceeds max",
			yaml:            "collectors:\n  pg_stat_statements:\n    diff_limit: 501",
			wantErrContains: "diff_limit",
		},
		{
			name:            "negative diff_limit rejected",
			yaml:            "collectors:\n  pg_stat_statements:\n    diff_limit: -1",
			wantErrContains: "diff_limit",
		},
		{
			name:            "max_query_text_length exceeds max",
			yaml:            "collectors:\n  pg_stat_statements:\n    max_query_text_length: 8193",
			wantErrContains: "max_query_text_length",
		},
		{
			name:            "negative backfill_batch_size rejected",
			yaml:            "collectors:\n  pg_class:\n    backfill_batch_size: -1",
			wantErrContains: "backfill_batch_size",
		},
		{
			name:            "negative category_limit rejected",
			yaml:            "collectors:\n  pg_stat_user_tables:\n    category_limit: -1",
			wantErrContains: "category_limit",
		},
		{
			name:            "negative interval_seconds rejected",
			yaml:            "collectors:\n  pg_class:\n    interval_seconds: -1",
			wantErrContains: "interval_seconds",
		},
		{
			name:            "env var exceeding max rejected",
			envVars:         map[string]string{"DBT_COLLECTOR_PG_STAT_STATEMENTS_DIFF_LIMIT": "501"},
			wantErrContains: "diff_limit",
		},
		{
			name:            "env var below min rejected",
			envVars:         map[string]string{"DBT_COLLECTOR_PG_CLASS_BACKFILL_BATCH_SIZE": "-1"},
			wantErrContains: "backfill_batch_size",
		},
		{
			name:            "env var max_query_text_length exceeding max",
			envVars:         map[string]string{"DBT_COLLECTOR_PG_STAT_STATEMENTS_MAX_QUERY_TEXT_LENGTH": "8193"},
			wantErrContains: "max_query_text_length",
		},
		{
			name:            "database_average_query_runtime env var exceeding max",
			envVars:         map[string]string{"DBT_COLLECTOR_DATABASE_AVERAGE_QUERY_RUNTIME_MAX_QUERY_TEXT_LENGTH": "8193"},
			wantErrContains: "max_query_text_length",
		},

		// --- Boundary values (accepted) ---
		{
			name: "boundary: diff_limit at zero",
			yaml: "collectors:\n  pg_stat_statements:\n    diff_limit: 0",
		},
		{
			name: "boundary: diff_limit at max",
			yaml: "collectors:\n  pg_stat_statements:\n    diff_limit: 500",
		},
		{
			name: "boundary: interval_seconds at zero",
			yaml: "collectors:\n  pg_class:\n    interval_seconds: 0",
		},
	})
}
