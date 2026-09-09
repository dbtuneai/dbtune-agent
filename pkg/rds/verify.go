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

// targetKnob is one parameter the agent asks RDS to set, carrying what is
// needed both to write it and to verify afterwards that it landed. Values are
// formatted in the parameter's pg_settings native unit, which is also the unit
// RDS parameter groups use.
type targetKnob struct {
	Name string
	// Value is the requested setting, formatted as pg_settings would report it.
	Value string
	// Vartype is the pg_settings vartype carried with the proposal, used to
	// compare values without tripping over representation differences.
	Vartype string
}

// targetKnobsToApply resolves every overridden knob in the proposal into a
// targetKnob. It fails as a unit: either every knob parses or none is applied.
func targetKnobsToApply(proposedConfig *agent.ProposedConfigResponse) ([]targetKnob, error) {
	targets := make([]targetKnob, 0, len(proposedConfig.KnobsOverrides))
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return nil, fmt.Errorf("failed to find recommended knob: %w", err)
		}
		value, err := knobConfig.GetSettingValue()
		if err != nil {
			return nil, fmt.Errorf("failed to get setting value: %w", err)
		}
		targets = append(targets, targetKnob{
			Name:    knobConfig.Name,
			Value:   value,
			Vartype: knobConfig.Vartype,
		})
	}
	return targets, nil
}

// awsParameters renders the targets as ModifyDBParameterGroup input.
func awsParameters(targets []targetKnob, applyMethod rdsTypes.ApplyMethod) []rdsTypes.Parameter {
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

// targetNames lists the parameter names, for the DescribeDBParameters filter
// and for error messages.
func targetNames(targets []targetKnob) []string {
	names := make([]string, 0, len(targets))
	for _, t := range targets {
		names = append(names, t.Name)
	}
	return names
}

// groupValueMismatches reports the requested parameters the group does not
// hold, formatted for an error message.
//
// This only runs once an apply has already failed, to tell a write that never
// stuck apart from one the engine simply did not load. Do not use it to judge
// an apply: the group can hold a value the running server has never seen,
// which is what diffPGSettings is for.
func groupValueMismatches(targets []targetKnob, actual []rdsTypes.Parameter) []string {
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
		// ParameterValue is omitted entirely when a parameter has never been
		// set, so an unset parameter reads as "" and never matches.
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

// settingsDiff is what pg_settings says about the parameters the agent wrote.
// The zero value means the running server reports every requested value.
type settingsDiff struct {
	// Missing knobs are absent from pg_settings entirely (unknown parameter
	// name for this server version).
	Missing []string
	// Mismatched knobs report a value other than the one requested. Right
	// after a write this is the normal "not propagated yet" state; once the
	// wait times out it means the value never arrived.
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

// diffPGSettings compares the requested values against what the running
// server reports. This is the only check that proves an apply actually took
// effect: the parameter group can hold a value the engine never loaded.
func diffPGSettings(targets []targetKnob, rows []queries.PgSettingsRow) settingsDiff {
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
		// Prefer the vartype the server reports over the proposal's.
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

// valuesEqual compares two settings written in the same unit, tolerating the
// representation differences between what the agent sends and what pg_settings
// reports (e.g. "1" vs "1.0", "on" vs "true", "LOGICAL" vs "logical").
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
			// The agent formats reals with %.6g, so compare on that precision
			// rather than requiring bit equality.
			return math.Abs(w-g) <= math.Max(1e-9, 1e-6*math.Abs(w))
		}
	case "bool":
		w, wOK := parseBool(want)
		g, gOK := parseBool(got)
		if wOK && gOK {
			return w == g
		}
	}

	// Enums and strings: PostgreSQL lowercases most enum values.
	return strings.EqualFold(want, got)
}

// parseNumber parses an integer setting, accepting a float representation of a
// whole number ("1024.0") since JSON round-trips can introduce one.
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
