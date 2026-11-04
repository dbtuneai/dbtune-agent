package aiven

import (
	"testing"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOverrideWorkMemKBtoMB_Int64(t *testing.T) {
	rows := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "shared_buffers", Setting: int64(131072), Unit: "8kB", Vartype: "integer", Context: "postmaster"},
		agent.PGConfigRow{Name: "work_mem", Setting: int64(4096), Unit: "kB", Vartype: "integer", Context: "user"},
		agent.PGConfigRow{Name: "max_connections", Setting: int64(100), Unit: nil, Vartype: "integer", Context: "postmaster"},
	}

	got, err := overrideWorkMemKBtoMB(rows)
	require.NoError(t, err)
	require.Len(t, got, 3)

	wm := got[1].(agent.PGConfigRow)
	assert.Equal(t, "work_mem", wm.Name)
	assert.Equal(t, 4, wm.Setting)         // 4096 kB → 4 MB
	assert.Equal(t, "MB", wm.Unit)         // unit overridden
	assert.Equal(t, "integer", wm.Vartype) // preserved
	assert.Equal(t, "service", wm.Context) // overridden to service

	// Other rows untouched.
	assert.Equal(t, int64(131072), got[0].(agent.PGConfigRow).Setting)
	assert.Equal(t, int64(100), got[2].(agent.PGConfigRow).Setting)
}

func TestOverrideWorkMemKBtoMB_String(t *testing.T) {
	rows := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "work_mem", Setting: "8192", Unit: "kB", Vartype: "integer", Context: "user"},
	}

	got, err := overrideWorkMemKBtoMB(rows)
	require.NoError(t, err)

	wm := got[0].(agent.PGConfigRow)
	assert.Equal(t, 8, wm.Setting) // 8192 kB → 8 MB
}

func TestOverrideWorkMemKBtoMB_UnexpectedType(t *testing.T) {
	rows := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "work_mem", Setting: float64(4096.5), Unit: "kB", Vartype: "integer", Context: "user"},
	}

	_, err := overrideWorkMemKBtoMB(rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected work_mem type")
}

func TestOverrideWorkMemKBtoMB_UnparseableString(t *testing.T) {
	rows := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "work_mem", Setting: "not_a_number", Unit: "kB", Vartype: "integer", Context: "user"},
	}

	_, err := overrideWorkMemKBtoMB(rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error converting work_mem to int64")
}

func TestOverrideWorkMemKBtoMB_NoWorkMem(t *testing.T) {
	rows := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "shared_buffers", Setting: int64(131072), Unit: "8kB", Vartype: "integer", Context: "postmaster"},
	}

	got, err := overrideWorkMemKBtoMB(rows)
	require.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Equal(t, "shared_buffers", got[0].(agent.PGConfigRow).Name)
}

func TestOverrideWorkMemKBtoMB_Rounding(t *testing.T) {
	// 1023 kB → 0 MB (integer division truncates)
	rows := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "work_mem", Setting: int64(1023), Unit: "kB", Vartype: "integer", Context: "user"},
	}

	got, err := overrideWorkMemKBtoMB(rows)
	require.NoError(t, err)
	assert.Equal(t, 0, got[0].(agent.PGConfigRow).Setting)
}

func TestPrependSharedBuffersPercentage(t *testing.T) {
	rows := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "work_mem", Setting: int64(4096), Unit: "kB", Vartype: "integer", Context: "user"},
	}

	got := prependSharedBuffersPercentage(rows, 25.0)
	require.Len(t, got, 2)

	sbp := got[0].(agent.PGConfigRow)
	assert.Equal(t, "shared_buffers_percentage", sbp.Name)
	assert.Equal(t, 25.0, sbp.Setting)
	assert.Equal(t, "percentage", sbp.Unit)
	assert.Equal(t, "real", sbp.Vartype)
	assert.Equal(t, "service", sbp.Context)

	// Original row is now at index 1.
	assert.Equal(t, "work_mem", got[1].(agent.PGConfigRow).Name)
}

func TestPrependSharedBuffersPercentage_Default(t *testing.T) {
	got := prependSharedBuffersPercentage(nil, DEFAULT_SHARED_BUFFERS_PERCENTAGE)
	require.Len(t, got, 1)

	sbp := got[0].(agent.PGConfigRow)
	assert.Equal(t, float64(DEFAULT_SHARED_BUFFERS_PERCENTAGE), sbp.Setting)
}
