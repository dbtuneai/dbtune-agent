package rds

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupValueMismatches(t *testing.T) {
	targets := []configInfo{
		{Name: "work_mem", Value: "16384", Vartype: "integer"},
		{Name: "random_page_cost", Value: "1.1", Vartype: "real"},
	}
	param := func(name string, value *string) rdsTypes.Parameter {
		p := rdsTypes.Parameter{ParameterName: aws.String(name)}
		if value != nil {
			p.ParameterValue = value
		}
		return p
	}

	t.Run("group holds everything", func(t *testing.T) {
		assert.Empty(t, groupValueMismatches(targets, []rdsTypes.Parameter{
			param("work_mem", aws.String("16384")),
			// Same value, different representation.
			param("random_page_cost", aws.String("1.100")),
			param("unrelated", aws.String("x")),
		}))
	})

	t.Run("reports the stored value", func(t *testing.T) {
		got := groupValueMismatches(targets, []rdsTypes.Parameter{
			param("work_mem", aws.String("4096")),
			param("random_page_cost", aws.String("1.1")),
		})
		require.Len(t, got, 1)
		assert.Equal(t, "work_mem (group has 4096)", got[0])
	})

	t.Run("never set reads as unset", func(t *testing.T) {
		got := groupValueMismatches(targets[:1], []rdsTypes.Parameter{param("work_mem", nil)})
		require.Len(t, got, 1)
		assert.Equal(t, "work_mem (group has <unset>)", got[0])
	})

	t.Run("absent from the group", func(t *testing.T) {
		got := groupValueMismatches(targets[:1], nil)
		require.Len(t, got, 1)
		assert.Equal(t, "work_mem (absent from the group)", got[0])
	})
}

func TestDiffPGSettings(t *testing.T) {
	targets := []configInfo{
		{Name: "work_mem", Value: "16384", Vartype: "integer"},
		{Name: "shared_buffers", Value: "262144", Vartype: "integer"},
	}

	row := func(name, setting, vartype string) queries.PgSettingsRow {
		return queries.PgSettingsRow{
			Name:    queries.Text(name),
			Setting: queries.Text(setting),
			Vartype: queries.Text(vartype),
		}
	}

	t.Run("server reports both values", func(t *testing.T) {
		diff := diffPGSettings(targets, []queries.PgSettingsRow{
			row("work_mem", "16384", "integer"),
			row("shared_buffers", "262144", "integer"),
			row("max_connections", "100", "integer"),
		})
		assert.True(t, diff.applied(), "diff: %s", diff)
	})

	t.Run("value never arrived", func(t *testing.T) {
		diff := diffPGSettings(targets, []queries.PgSettingsRow{
			row("work_mem", "4096", "integer"),
			row("shared_buffers", "262144", "integer"),
		})
		require.Len(t, diff.Mismatched, 1)
		assert.Contains(t, diff.Mismatched[0], "work_mem")
	})

	t.Run("unknown to this server", func(t *testing.T) {
		diff := diffPGSettings(
			[]configInfo{{Name: "made_up_guc", Value: "1", Vartype: "integer"}},
			[]queries.PgSettingsRow{row("work_mem", "16384", "integer")},
		)
		assert.Equal(t, []string{"made_up_guc"}, diff.Missing)
	})

	t.Run("server vartype wins over the proposal's", func(t *testing.T) {
		// Proposal says integer, server says real.
		diff := diffPGSettings(
			[]configInfo{{Name: "random_page_cost", Value: "1.1", Vartype: "integer"}},
			[]queries.PgSettingsRow{row("random_page_cost", "1.1000", "real")},
		)
		assert.True(t, diff.applied(), "diff: %s", diff)
	})
}

func TestValuesEqual(t *testing.T) {
	cases := []struct {
		vartype, want, got string
		equal              bool
	}{
		{"integer", "16384", "16384", true},
		{"integer", "16384", " 16384 ", true},
		{"integer", "16384", "16384.0", true},
		{"integer", "16384", "8192", false},
		{"real", "1.1", "1.1000", true},
		{"real", "1.1", "1.10000001", true},
		{"real", "1.1", "1.2", false},
		{"real", "0", "0.0", true},
		{"bool", "on", "true", true},
		{"bool", "off", "0", true},
		{"bool", "on", "off", false},
		{"enum", "logical", "LOGICAL", true},
		{"string", "pg_stat_statements", "pg_stat_statements", true},
		{"string", "pg_stat_statements", "pg_stat_statements,auto_explain", false},
		// An unset parameter never matches a requested value.
		{"integer", "16384", "", false},
		{"string", "", "", true},
		// Both failing to parse must not compare equal.
		{"integer", "auto", "16384", false},
	}
	for _, c := range cases {
		assert.Equal(t, c.equal, valuesEqual(c.vartype, c.want, c.got),
			"valuesEqual(%q, %q, %q)", c.vartype, c.want, c.got)
	}
}

func TestConfigInfoChanged(t *testing.T) {
	cases := []struct {
		name    string
		config  configInfo
		changed bool
	}{
		{
			name:    "group already holds the value",
			config:  configInfo{Name: "work_mem", Value: "16384", Vartype: "integer", CurrentRDSValue: "16384"},
			changed: false,
		},
		{
			name:    "same value, different representation",
			config:  configInfo{Name: "random_page_cost", Value: "1.1", Vartype: "real", CurrentRDSValue: "1.100"},
			changed: false,
		},
		{
			name:    "different value",
			config:  configInfo{Name: "work_mem", Value: "16384", Vartype: "integer", CurrentRDSValue: "4096"},
			changed: true,
		},
		{
			name: "absent from the group reads as changed",
			// getConfigInfo leaves the zero value when the group has no such
			// parameter, so we write it rather than silently skipping it.
			config:  configInfo{Name: "work_mem", Value: "16384", Vartype: "integer"},
			changed: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.changed, c.config.changed())
		})
	}
}

func TestConfigInfoIsChangedRestartParameter(t *testing.T) {
	cases := []struct {
		name   string
		config configInfo
		want   bool
	}{
		{
			name:   "static parameter with a new value",
			config: configInfo{Name: "shared_buffers", Value: "262144", Vartype: "integer", CurrentRDSValue: "131072", RequiresReboot: true},
			want:   true,
		},
		{
			name: "static parameter already at the requested value",
			// Nothing to reboot for: the value is already in the group.
			config: configInfo{Name: "shared_buffers", Value: "262144", Vartype: "integer", CurrentRDSValue: "262144", RequiresReboot: true},
			want:   false,
		},
		{
			name:   "dynamic parameter with a new value",
			config: configInfo{Name: "work_mem", Value: "16384", Vartype: "integer", CurrentRDSValue: "4096"},
			want:   false,
		},
		{
			name:   "dynamic parameter already at the requested value",
			config: configInfo{Name: "work_mem", Value: "16384", Vartype: "integer", CurrentRDSValue: "16384"},
			want:   false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, c.config.isChangedRestartParameter())
		})
	}
}

// The adapter picks the apply method from the batch, not from a single knob:
// one changed static parameter forces the reboot path for all of them.
func TestChangedRestartParameterInBatch(t *testing.T) {
	dynamic := configInfo{Name: "work_mem", Value: "16384", Vartype: "integer", CurrentRDSValue: "4096"}
	staticUnchanged := configInfo{Name: "shared_buffers", Value: "262144", Vartype: "integer", CurrentRDSValue: "262144", RequiresReboot: true}
	staticChanged := configInfo{Name: "max_connections", Value: "200", Vartype: "integer", CurrentRDSValue: "100", RequiresReboot: true}

	assert.False(t, slices.ContainsFunc(
		[]configInfo{dynamic, staticUnchanged}, configInfo.isChangedRestartParameter))
	assert.True(t, slices.ContainsFunc(
		[]configInfo{dynamic, staticUnchanged, staticChanged}, configInfo.isChangedRestartParameter))
	assert.False(t, slices.ContainsFunc([]configInfo(nil), configInfo.isChangedRestartParameter))
}

func TestClassifyInstanceStatus(t *testing.T) {
	cases := []struct {
		status string
		want   instanceStatusClass
	}{
		{"available", instanceStatusServing},
		// The engine serves through all of these; the SDK waiter did not accept
		// them, which is how a fine apply turned into a 15 minute timeout.
		{"backing-up", instanceStatusServing},
		{"storage-optimization", instanceStatusServing},
		{"configuring-enhanced-monitoring", instanceStatusServing},
		{"Available", instanceStatusServing},
		{"rebooting", instanceStatusBusy},
		{"modifying", instanceStatusBusy},
		{"upgrading", instanceStatusBusy},
		// A status RDS adds later should cost a wait, not a spurious failure.
		{"some-future-status", instanceStatusBusy},
		{"", instanceStatusBusy},
		{"incompatible-parameters", instanceStatusTerminal},
		{"failed", instanceStatusTerminal},
		{"deleting", instanceStatusTerminal},
	}
	for _, c := range cases {
		t.Run(c.status, func(t *testing.T) {
			assert.Equal(t, c.want, classifyInstanceStatus(c.status))
		})
	}
}
