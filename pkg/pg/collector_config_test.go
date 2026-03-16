package pg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(v bool) *bool { return &v }
func intPtr(v int) *int    { return &v }

// --- IsEnabled ---

func TestCollectorOverride_IsEnabled_NilDefault(t *testing.T) {
	o := CollectorOverride{}
	assert.True(t, o.IsEnabled())
}

func TestCollectorOverride_IsEnabled_True(t *testing.T) {
	o := CollectorOverride{Enabled: boolPtr(true)}
	assert.True(t, o.IsEnabled())
}

func TestCollectorOverride_IsEnabled_False(t *testing.T) {
	o := CollectorOverride{Enabled: boolPtr(false)}
	assert.False(t, o.IsEnabled())
}

// --- IntervalOr ---

func TestCollectorOverride_IntervalOr_Nil(t *testing.T) {
	o := CollectorOverride{}
	assert.Equal(t, 5*time.Second, o.IntervalOr(5*time.Second))
}

func TestCollectorOverride_IntervalOr_AboveDefault(t *testing.T) {
	o := CollectorOverride{IntervalSeconds: intPtr(10)}
	assert.Equal(t, 10*time.Second, o.IntervalOr(5*time.Second))
}

func TestCollectorOverride_IntervalOr_BelowDefault_ClampedToDefault(t *testing.T) {
	o := CollectorOverride{IntervalSeconds: intPtr(3)}
	// Configured 3s but default is 5s — must clamp to 5s
	assert.Equal(t, 5*time.Second, o.IntervalOr(5*time.Second))
}

func TestCollectorOverride_IntervalOr_EqualToDefault(t *testing.T) {
	o := CollectorOverride{IntervalSeconds: intPtr(5)}
	assert.Equal(t, 5*time.Second, o.IntervalOr(5*time.Second))
}

// --- IntOr / BoolOr ---

func TestIntOr(t *testing.T) {
	assert.Equal(t, 42, IntOr(intPtr(42), 100))
	assert.Equal(t, 100, IntOr(nil, 100))
}

func TestBoolOr(t *testing.T) {
	assert.True(t, BoolOr(boolPtr(true), false))
	assert.False(t, BoolOr(boolPtr(false), true))
	assert.True(t, BoolOr(nil, true))
	assert.False(t, BoolOr(nil, false))
}

// --- validateCollectorsConfig ---

func TestValidateCollectorsConfig_Valid(t *testing.T) {
	cfg := CollectorsConfig{
		"pg_stat_statements": {
			IntervalSeconds: intPtr(10),
			DiffLimit:       intPtr(100),
		},
		"pg_stats": {
			Enabled:           boolPtr(false),
			BackfillBatchSize: intPtr(500),
		},
	}
	err := validateCollectorsConfig(cfg)
	assert.NoError(t, err)
}

func TestValidateCollectorsConfig_UnknownCollector(t *testing.T) {
	cfg := CollectorsConfig{
		"nonexistent_collector": {},
	}
	err := validateCollectorsConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown collector")
}

func TestValidateCollectorsConfig_NegativeInterval(t *testing.T) {
	cfg := CollectorsConfig{
		"pg_stats": {IntervalSeconds: intPtr(-1)},
	}
	err := validateCollectorsConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "interval_seconds must be > 0")
}

func TestValidateCollectorsConfig_NegativeBatchSize(t *testing.T) {
	cfg := CollectorsConfig{
		"pg_stats": {BackfillBatchSize: intPtr(-1)},
	}
	err := validateCollectorsConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backfill_batch_size must be >= 0")
}

func TestValidateCollectorsConfig_NegativeDiffLimit(t *testing.T) {
	cfg := CollectorsConfig{
		"pg_stat_statements": {DiffLimit: intPtr(-1)},
	}
	err := validateCollectorsConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "diff_limit must be >= 0")
}

func TestValidateCollectorsConfig_DiffLimitAboveMax(t *testing.T) {
	cfg := CollectorsConfig{
		"pg_stat_statements": {DiffLimit: intPtr(MaxDiffLimit + 1)},
	}
	err := validateCollectorsConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "diff_limit must be <= 500")
}

func TestValidateCollectorsConfig_DiffLimitAtMax(t *testing.T) {
	cfg := CollectorsConfig{
		"pg_stat_statements": {DiffLimit: intPtr(MaxDiffLimit)},
	}
	err := validateCollectorsConfig(cfg)
	assert.NoError(t, err)
}

func TestValidateCollectorsConfig_MaxQueryTextLengthAboveMax(t *testing.T) {
	cfg := CollectorsConfig{
		"pg_stat_statements": {MaxQueryTextLength: intPtr(MaxQueryTextLength + 1)},
	}
	err := validateCollectorsConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_query_text_length must be <= 8192")
}

func TestValidateCollectorsConfig_MaxQueryTextLengthAtMax(t *testing.T) {
	cfg := CollectorsConfig{
		"pg_stat_statements": {MaxQueryTextLength: intPtr(MaxQueryTextLength)},
	}
	err := validateCollectorsConfig(cfg)
	assert.NoError(t, err)
}

func TestValidateCollectorsConfig_NegativeCategoryLimit(t *testing.T) {
	cfg := CollectorsConfig{
		"pg_stat_user_tables": {CategoryLimit: intPtr(-1)},
	}
	err := validateCollectorsConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "category_limit must be >= 0")
}

func TestValidateCollectorsConfig_Empty(t *testing.T) {
	cfg := CollectorsConfig{}
	err := validateCollectorsConfig(cfg)
	assert.NoError(t, err)
}

// --- KnownCollectorNames ---

func TestKnownCollectorNames_Contains37(t *testing.T) {
	names := KnownCollectorNames()
	assert.Len(t, names, 37)
}
