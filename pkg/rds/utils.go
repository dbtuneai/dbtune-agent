package rds

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/pg/queries"
)

// configInfo is one parameter that we want to update, paired with what the
// parameter group currently holds for it. We fetch the data early
type configInfo struct {
	Name            string
	Value           string
	Vartype         string
	CurrentRDSValue string
	RequiresReboot  bool
}

func (c configInfo) changed() bool {
	return !valuesEqual(c.Vartype, c.Value, c.CurrentRDSValue)
}

func getConfigInfo(
	proposedConfig *agent.ProposedConfigResponse,
	clients *AWSClients,
	parameterGroupName string,
	ctx context.Context,
) ([]configInfo, error) {
	configs, err := extractConfigValues(proposedConfig)
	if err != nil {
		return nil, err
	}

	rdsParameters, err := getRDSParameterInfo(clients, parameterGroupName, getConfigNames(configs), ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read parameter group %q: %w", parameterGroupName, err)
	}
	group := make(map[string]rdsTypes.Parameter, len(rdsParameters))
	for _, p := range rdsParameters {
		group[aws.ToString(p.ParameterName)] = p
	}

	for i, c := range configs {
		// Absent from the response means the engine has no such parameter. That
		// leaves the zero values, which read downstream as a mismatch.
		p := group[c.Name]
		configs[i].CurrentRDSValue = aws.ToString(p.ParameterValue)
		configs[i].RequiresReboot = aws.ToString(p.ApplyType) == "static"
	}
	return configs, nil
}

// extractConfigValues pulls the knobs to write out of the proposal. Kept pure so
// it stays testable without AWS.
func extractConfigValues(proposedConfig *agent.ProposedConfigResponse) ([]configInfo, error) {
	// using KnobsOverrides here is for backwardscompatability
	// KnobsOverrides and Config holds the same values
	configs := make([]configInfo, 0, len(proposedConfig.KnobsOverrides))
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return nil, fmt.Errorf("failed to find recommended knob: %w", err)
		}
		value, err := knobConfig.GetSettingValue()
		if err != nil {
			return nil, fmt.Errorf("failed to get setting value: %w", err)
		}
		configs = append(configs, configInfo{
			Name:    knobConfig.Name,
			Value:   value,
			Vartype: knobConfig.Vartype,
		})
	}
	return configs, nil
}

// awsParameters renders the targets as ModifyDBParameterGroup input.
func awsParameters(targets []configInfo, applyMethod rdsTypes.ApplyMethod) []rdsTypes.Parameter {
	params := make([]rdsTypes.Parameter, 0, len(targets))
	for _, t := range targets {
		params = append(params, rdsTypes.Parameter{
			ParameterName:  aws.String(t.Name),
			ParameterValue: aws.String(t.Value),
			ApplyMethod:    applyMethod,
		})
	}
	return params
}

func getConfigNames(config []configInfo) []string {
	names := make([]string, len(config))
	for i, c := range config {
		names[i] = c.Name
	}
	return names
}

// builds a map with the targets as keys and fills it up with values from the param group
// and then it compares that map with the target values.
func groupValueMismatches(targets []configInfo, actual []rdsTypes.Parameter) []string {
	// ParameterValue is omitted when never set, so it reads as "".
	valueMap := make(map[string]string, len(actual))
	for _, p := range actual {
		valueMap[aws.ToString(p.ParameterName)] = aws.ToString(p.ParameterValue)
	}

	var mismatches []string
	for _, t := range targets {
		switch current, ok := valueMap[t.Name]; {
		case !ok:
			// Absent means the engine lacks the parameter, not that it is unset.
			mismatches = append(mismatches, t.Name+" (absent from the group)")
		case !valuesEqual(t.Vartype, t.Value, current):
			mismatches = append(mismatches, t.Name+" (group has "+cmp.Or(current, "<unset>")+")")
		}
	}
	return mismatches
}

// settingsDiff is what pg_settings reports about the parameters written. The
// zero value means every requested value is live.
type settingsDiff struct {
	// Missing is absent from pg_settings: unknown to this server version.
	Missing []string
	// Mismatched reports another value. Normal right after a write; after the
	// wait times out, the value never arrived.
	Mismatched []string
}

func (d settingsDiff) applied() bool {
	return len(d.Missing) == 0 && len(d.Mismatched) == 0
}

func (d settingsDiff) String() string {
	var parts []string
	if len(d.Missing) > 0 {
		parts = append(parts, "unknown to this server: "+strings.Join(d.Missing, ", "))
	}
	if len(d.Mismatched) > 0 {
		parts = append(parts, "not yet reported: "+strings.Join(d.Mismatched, ", "))
	}
	if len(parts) == 0 {
		return "all settings applied"
	}
	return strings.Join(parts, "; ")
}

func diffPGSettings(targets []configInfo, rows []queries.PgSettingsRow) settingsDiff {
	byName := make(map[string]queries.PgSettingsRow, len(rows))
	for _, r := range rows {
		byName[string(r.Name)] = r
	}

	var diff settingsDiff
	for _, t := range targets {
		row, ok := byName[t.Name]
		// The server's vartype wins over the proposal's.
		vartype := cmp.Or(string(row.Vartype), t.Vartype)
		switch {
		case !ok:
			diff.Missing = append(diff.Missing, t.Name)
		case !valuesEqual(vartype, t.Value, string(row.Setting)):
			diff.Mismatched = append(diff.Mismatched, fmt.Sprintf(
				"%s (want %s, server reports %s)", t.Name, t.Value, row.Setting))
		}
	}
	return diff
}

// valuesEqual compares two settings in the same unit, tolerating
// representation differences: "1" vs "1.0", "on" vs "true", "LOGICAL" vs
// "logical".
func valuesEqual(vartype, want, got string) bool {
	want = strings.TrimSpace(want)
	got = strings.TrimSpace(got)
	if want == got {
		return true
	}
	if want == "" || got == "" {
		return false
	}

	switch strings.ToLower(vartype) {
	case "integer":
		w, wErr := parseNumber(want)
		g, gErr := parseNumber(got)
		if wErr == nil && gErr == nil {
			return w == g
		}
	case "real":
		w, wErr := strconv.ParseFloat(want, 64)
		g, gErr := strconv.ParseFloat(got, 64)
		if wErr == nil && gErr == nil {
			// The agent formats reals with %.6g, so compare at that precision.
			return math.Abs(w-g) <= math.Max(1e-9, 1e-6*math.Abs(w))
		}
	case "bool":
		w, wOK := parseBool(want)
		g, gOK := parseBool(got)
		if wOK && gOK {
			return w == g
		}
	}

	// Enums and strings. PostgreSQL lowercases most enum values.
	return strings.EqualFold(want, got)
}

// parseNumber parses an integer setting, accepting "1024.0" from a JSON
// round-trip.
func parseNumber(s string) (int64, error) {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	if f != math.Trunc(f) {
		return 0, fmt.Errorf("not an integer value: %s", s)
	}
	return int64(f), nil
}

func parseBool(s string) (bool, bool) {
	switch strings.ToLower(s) {
	case "on", "true", "yes", "1":
		return true, true
	case "off", "false", "no", "0":
		return false, true
	}
	return false, false
}
