package rds

import (
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

// configValue is one parameter to write and later verify.
// format of proposedConfig is bloated extract what we need
type configValue struct {
	Name    string
	Value   string
	Vartype string
}

func extractConfigValues(proposedConfig *agent.ProposedConfigResponse) ([]configValue, error) {
	targets := make([]configValue, 0, len(proposedConfig.KnobsOverrides))
	// using KnobsOverrides here is for backwardscompatability
	// KnobsOverrides and Config holds the same values
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return nil, fmt.Errorf("failed to find recommended knob: %w", err)
		}
		value, err := knobConfig.GetSettingValue()
		if err != nil {
			return nil, fmt.Errorf("failed to get setting value: %w", err)
		}
		targets = append(targets, configValue{
			Name:    knobConfig.Name,
			Value:   value,
			Vartype: knobConfig.Vartype,
		})
	}
	return targets, nil
}

// awsParameters renders the targets as ModifyDBParameterGroup input.
func awsParameters(targets []configValue, applyMethod rdsTypes.ApplyMethod) []rdsTypes.Parameter {
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

func configNames(config []configValue) []string {
	names := make([]string, 0, len(config))
	for _, t := range config {
		names = append(names, t.Name)
	}
	return names
}

// groupValueMismatches reports which requested values the group does not hold.
// Diagnostic only: the group can hold a value the server never loaded, so use
// diffPGSettings to judge an apply.
func groupValueMismatches(targets []configValue, actual []rdsTypes.Parameter) []string {
	byName := make(map[string]rdsTypes.Parameter, len(actual))
	for _, p := range actual {
		byName[aws.ToString(p.ParameterName)] = p
	}

	var mismatches []string
	for _, t := range targets {
		param, ok := byName[t.Name]
		if !ok {
			mismatches = append(mismatches, fmt.Sprintf("%s (absent from the group)", t.Name))
			continue
		}
		// ParameterValue is omitted when never set, so it reads as "".
		current := aws.ToString(param.ParameterValue)
		if valuesEqual(t.Vartype, t.Value, current) {
			continue
		}
		if current == "" {
			current = "<unset>"
		}
		mismatches = append(mismatches, fmt.Sprintf("%s (group has %s)", t.Name, current))
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

// diffPGSettings compares the requested values against what the server
// reports. Only this proves an apply took effect.
func diffPGSettings(targets []configValue, rows []queries.PgSettingsRow) settingsDiff {
	byName := make(map[string]queries.PgSettingsRow, len(rows))
	for _, r := range rows {
		byName[string(r.Name)] = r
	}

	var diff settingsDiff
	for _, t := range targets {
		row, ok := byName[t.Name]
		if !ok {
			diff.Missing = append(diff.Missing, t.Name)
			continue
		}
		// The server's vartype wins over the proposal's.
		vartype := t.Vartype
		if row.Vartype != "" {
			vartype = string(row.Vartype)
		}
		if valuesEqual(vartype, t.Value, string(row.Setting)) {
			continue
		}
		diff.Mismatched = append(diff.Mismatched, fmt.Sprintf(
			"%s (want %s, server reports %s)", t.Name, t.Value, string(row.Setting),
		))
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
