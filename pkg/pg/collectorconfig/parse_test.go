package collectorconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBoolValue(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    bool
		wantErr bool
	}{
		{"bool true", true, true, false},
		{"bool false", false, false, false},
		{"string true", "true", true, false},
		{"string false", "false", false, false},
		{"string TRUE", "TRUE", true, false},
		{"string False", "False", false, false},
		{"string 1", "1", true, false},
		{"string 0", "0", false, false},
		{"invalid string", "notabool", false, true},
		{"int rejected", 1, false, true},
		{"float rejected", 1.0, false, true},
		{"nil rejected", nil, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBoolValue(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParseIntValue(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    int
		wantErr bool
	}{
		{"int", 42, 42, false},
		{"int zero", 0, 0, false},
		{"int negative", -5, -5, false},
		{"int64", int64(42), 42, false},
		{"float64 integer", float64(42), 42, false},
		{"float64 zero", float64(0), 0, false},
		{"float64 with decimal", 42.5, 0, true},
		{"string", "42", 42, false},
		{"string negative", "-3", -3, false},
		{"string zero", "0", 0, false},
		{"invalid string", "abc", 0, true},
		{"empty string", "", 0, true},
		{"bool rejected", true, 0, true},
		{"nil rejected", nil, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseIntValue(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

type testConfig struct {
	Limit   int  `config:"limit" default:"100" min:"0" max:"500"`
	Enabled bool `config:"enabled"`
	Plain   int  `config:"plain"`
}

type minOnlyConfig struct {
	BatchSize int `config:"batch_size" default:"200" min:"0"`
}

type configWithUntaggedField struct {
	Tagged   int `config:"tagged"`
	Untagged int // no config tag — should be ignored
}

func TestParse(t *testing.T) {
	t.Run("nil raw returns defaults", func(t *testing.T) {
		cfg, err := Parse[testConfig](nil)
		require.NoError(t, err)
		assert.Equal(t, 100, cfg.Limit)
		assert.False(t, cfg.Enabled)
		assert.Equal(t, 0, cfg.Plain)
	})

	t.Run("empty raw returns defaults", func(t *testing.T) {
		cfg, err := Parse[testConfig](map[string]any{})
		require.NoError(t, err)
		assert.Equal(t, 100, cfg.Limit)
	})

	t.Run("sets values from raw", func(t *testing.T) {
		cfg, err := Parse[testConfig](map[string]any{
			"limit":   200,
			"enabled": true,
			"plain":   42,
		})
		require.NoError(t, err)
		assert.Equal(t, 200, cfg.Limit)
		assert.True(t, cfg.Enabled)
		assert.Equal(t, 42, cfg.Plain)
	})

	t.Run("rejects unknown field", func(t *testing.T) {
		_, err := Parse[testConfig](map[string]any{"bogus": 1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown field")
	})

	t.Run("rejects value below min", func(t *testing.T) {
		_, err := Parse[testConfig](map[string]any{"limit": -1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "limit")
	})

	t.Run("rejects value above max", func(t *testing.T) {
		_, err := Parse[testConfig](map[string]any{"limit": 501})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "limit")
	})

	t.Run("boundary: at min", func(t *testing.T) {
		cfg, err := Parse[testConfig](map[string]any{"limit": 0})
		require.NoError(t, err)
		assert.Equal(t, 0, cfg.Limit)
	})

	t.Run("boundary: at max", func(t *testing.T) {
		cfg, err := Parse[testConfig](map[string]any{"limit": 500})
		require.NoError(t, err)
		assert.Equal(t, 500, cfg.Limit)
	})

	t.Run("string values parsed (env var style)", func(t *testing.T) {
		cfg, err := Parse[testConfig](map[string]any{
			"limit":   "300",
			"enabled": "true",
		})
		require.NoError(t, err)
		assert.Equal(t, 300, cfg.Limit)
		assert.True(t, cfg.Enabled)
	})

	t.Run("invalid int type rejected", func(t *testing.T) {
		_, err := Parse[testConfig](map[string]any{"limit": "abc"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "limit")
	})

	t.Run("invalid bool type rejected", func(t *testing.T) {
		_, err := Parse[testConfig](map[string]any{"enabled": "notabool"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "enabled")
	})

	t.Run("min-only validation rejects negative", func(t *testing.T) {
		_, err := Parse[minOnlyConfig](map[string]any{"batch_size": -1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "batch_size")
	})

	t.Run("min-only validation allows large values", func(t *testing.T) {
		cfg, err := Parse[minOnlyConfig](map[string]any{"batch_size": 999999})
		require.NoError(t, err)
		assert.Equal(t, 999999, cfg.BatchSize)
	})

	t.Run("min-only default applied", func(t *testing.T) {
		cfg, err := Parse[minOnlyConfig](nil)
		require.NoError(t, err)
		assert.Equal(t, 200, cfg.BatchSize)
	})

	t.Run("fields without config tag are ignored", func(t *testing.T) {
		cfg, err := Parse[configWithUntaggedField](map[string]any{"tagged": 42})
		require.NoError(t, err)
		assert.Equal(t, 42, cfg.Tagged)
		assert.Equal(t, 0, cfg.Untagged) // untouched
	})

	t.Run("untagged field name as raw key is rejected", func(t *testing.T) {
		_, err := Parse[configWithUntaggedField](map[string]any{"Untagged": 5})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown field")
	})
}

type pointerConfig struct {
	Enabled *bool `config:"enabled"`
	Limit   *int  `config:"limit" min:"0" max:"100"`
}

type mixedConfig struct {
	Flag   bool `config:"flag"`
	OptVal *int `config:"opt_val" min:"0"`
}

func TestParsePointerFields(t *testing.T) {
	t.Run("nil raw returns all nil pointers", func(t *testing.T) {
		cfg, err := Parse[pointerConfig](nil)
		require.NoError(t, err)
		assert.Nil(t, cfg.Enabled)
		assert.Nil(t, cfg.Limit)
	})

	t.Run("sets pointer bool", func(t *testing.T) {
		cfg, err := Parse[pointerConfig](map[string]any{"enabled": true})
		require.NoError(t, err)
		require.NotNil(t, cfg.Enabled)
		assert.True(t, *cfg.Enabled)
	})

	t.Run("sets pointer int", func(t *testing.T) {
		cfg, err := Parse[pointerConfig](map[string]any{"limit": 42})
		require.NoError(t, err)
		require.NotNil(t, cfg.Limit)
		assert.Equal(t, 42, *cfg.Limit)
	})

	t.Run("string values parsed for pointers", func(t *testing.T) {
		cfg, err := Parse[pointerConfig](map[string]any{"enabled": "false", "limit": "50"})
		require.NoError(t, err)
		require.NotNil(t, cfg.Enabled)
		assert.False(t, *cfg.Enabled)
		require.NotNil(t, cfg.Limit)
		assert.Equal(t, 50, *cfg.Limit)
	})

	t.Run("min/max enforced on pointer int", func(t *testing.T) {
		_, err := Parse[pointerConfig](map[string]any{"limit": -1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "limit")

		_, err = Parse[pointerConfig](map[string]any{"limit": 101})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "limit")
	})

	t.Run("mixed pointer and non-pointer", func(t *testing.T) {
		cfg, err := Parse[mixedConfig](map[string]any{"flag": true, "opt_val": 10})
		require.NoError(t, err)
		assert.True(t, cfg.Flag)
		require.NotNil(t, cfg.OptVal)
		assert.Equal(t, 10, *cfg.OptVal)
	})

	t.Run("mixed with only non-pointer set", func(t *testing.T) {
		cfg, err := Parse[mixedConfig](map[string]any{"flag": true})
		require.NoError(t, err)
		assert.True(t, cfg.Flag)
		assert.Nil(t, cfg.OptVal)
	})
}

func TestParsePartial(t *testing.T) {
	t.Run("no unknown keys returns nil remaining", func(t *testing.T) {
		cfg, remaining, err := ParsePartial[testConfig](map[string]any{"limit": 200})
		require.NoError(t, err)
		assert.Equal(t, 200, cfg.Limit)
		assert.Nil(t, remaining)
	})

	t.Run("unknown keys returned in remaining", func(t *testing.T) {
		cfg, remaining, err := ParsePartial[testConfig](map[string]any{
			"limit": 200,
			"extra": "hello",
			"other": 42,
		})
		require.NoError(t, err)
		assert.Equal(t, 200, cfg.Limit)
		assert.Equal(t, map[string]any{"extra": "hello", "other": 42}, remaining)
	})

	t.Run("nil raw returns defaults and nil remaining", func(t *testing.T) {
		cfg, remaining, err := ParsePartial[testConfig](nil)
		require.NoError(t, err)
		assert.Equal(t, 100, cfg.Limit) // default
		assert.Nil(t, remaining)
	})

	t.Run("validation still applies", func(t *testing.T) {
		_, _, err := ParsePartial[testConfig](map[string]any{"limit": 501})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "limit")
	})

	t.Run("pointer fields with remaining", func(t *testing.T) {
		cfg, remaining, err := ParsePartial[pointerConfig](map[string]any{
			"enabled": true,
			"unknown": "val",
		})
		require.NoError(t, err)
		require.NotNil(t, cfg.Enabled)
		assert.True(t, *cfg.Enabled)
		assert.Nil(t, cfg.Limit)
		assert.Equal(t, map[string]any{"unknown": "val"}, remaining)
	})
}
