package tuning

import (
	"testing"

	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func textPtr(s string) *queries.Text {
	t := queries.Text(s)
	return &t
}

func TestSettingsToConfigRows_IntegerVartype(t *testing.T) {
	settings := []queries.PgSettingsRow{
		{Name: "work_mem", Setting: "4096", Vartype: "integer", Context: "user", Unit: textPtr("kB")},
	}
	got := settingsToConfigRows(settings)
	require.Len(t, got, 1)

	row := got[0].(PGConfigRow)
	assert.Equal(t, "work_mem", row.Name)
	assert.Equal(t, int64(4096), row.Setting) // InferNumericType: "4096" → int64
	assert.Equal(t, "kB", row.Unit)
	assert.Equal(t, "integer", row.Vartype)
	assert.Equal(t, "user", row.Context)
}

func TestSettingsToConfigRows_RealVartype(t *testing.T) {
	settings := []queries.PgSettingsRow{
		{Name: "seq_page_cost", Setting: "1.0", Vartype: "real", Context: "user", Unit: nil},
	}
	got := settingsToConfigRows(settings)
	require.Len(t, got, 1)

	row := got[0].(PGConfigRow)
	assert.Equal(t, float64(1.0), row.Setting) // InferNumericType: "1.0" → float64
	assert.Nil(t, row.Unit)                    // NULL unit → nil
}

func TestSettingsToConfigRows_StringVartype(t *testing.T) {
	settings := []queries.PgSettingsRow{
		{Name: "log_timezone", Setting: "UTC", Vartype: "string", Context: "sighup"},
	}
	got := settingsToConfigRows(settings)
	require.Len(t, got, 1)

	row := got[0].(PGConfigRow)
	assert.Equal(t, "UTC", row.Setting) // no InferNumericType for string vartype
	assert.Equal(t, "string", row.Vartype)
}

func TestSettingsToConfigRows_EnumVartype(t *testing.T) {
	settings := []queries.PgSettingsRow{
		{Name: "wal_level", Setting: "replica", Vartype: "enum", Context: "postmaster"},
	}
	got := settingsToConfigRows(settings)
	row := got[0].(PGConfigRow)
	assert.Equal(t, "replica", row.Setting)
}

func TestSettingsToConfigRows_BoolVartype(t *testing.T) {
	settings := []queries.PgSettingsRow{
		{Name: "jit", Setting: "on", Vartype: "bool", Context: "user"},
	}
	got := settingsToConfigRows(settings)
	row := got[0].(PGConfigRow)
	// "on" is not a numeric string, so it stays as string
	assert.Equal(t, "on", row.Setting)
}

func TestSettingsToConfigRows_NullUnit(t *testing.T) {
	settings := []queries.PgSettingsRow{
		{Name: "max_connections", Setting: "100", Vartype: "integer", Context: "postmaster", Unit: nil},
	}
	got := settingsToConfigRows(settings)
	row := got[0].(PGConfigRow)
	assert.Nil(t, row.Unit)
}

func TestSettingsToConfigRows_NonNullUnit(t *testing.T) {
	settings := []queries.PgSettingsRow{
		{Name: "shared_buffers", Setting: "16384", Vartype: "integer", Context: "postmaster", Unit: textPtr("8kB")},
	}
	got := settingsToConfigRows(settings)
	row := got[0].(PGConfigRow)
	assert.Equal(t, "8kB", row.Unit)
}

func TestSettingsToConfigRows_MixedRows(t *testing.T) {
	settings := []queries.PgSettingsRow{
		{Name: "max_connections", Setting: "100", Vartype: "integer", Context: "postmaster", Unit: nil},
		{Name: "seq_page_cost", Setting: "1.5", Vartype: "real", Context: "user", Unit: nil},
		{Name: "log_timezone", Setting: "UTC", Vartype: "string", Context: "sighup"},
		{Name: "wal_level", Setting: "replica", Vartype: "enum", Context: "postmaster"},
	}
	got := settingsToConfigRows(settings)
	require.Len(t, got, 4)

	assert.Equal(t, int64(100), got[0].(PGConfigRow).Setting)
	assert.Equal(t, float64(1.5), got[1].(PGConfigRow).Setting)
	assert.Equal(t, "UTC", got[2].(PGConfigRow).Setting)
	assert.Equal(t, "replica", got[3].(PGConfigRow).Setting)
}

func TestSettingsToConfigRows_Empty(t *testing.T) {
	got := settingsToConfigRows(nil)
	assert.Empty(t, got)
}
