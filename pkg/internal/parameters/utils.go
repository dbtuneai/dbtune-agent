package parameters

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dbtuneai/agent/pkg/agent"
)

type ParsedKnob struct {
	Name         string
	SettingValue string
}

// FindRecommendedKnob finds a recommended knob in recommendation array
func FindRecommendedKnob(config []agent.PGConfigRow, knob string) (agent.PGConfigRow, error) {
	for _, c := range config {
		if c.Name == knob {
			return c, nil
		}
	}
	return agent.PGConfigRow{}, fmt.Errorf("knob %s not found in configuration", knob)
}

// ParseKnobConfigurations parses and validates all knob configurations before applying any.
// This ensures that either all configurations are valid and can be applied, or none are applied
// if there's a parsing error, preventing partial configuration application.
func ParseKnobConfigurations(proposedConfig *agent.ProposedConfigResponse) ([]ParsedKnob, error) {
	var parsedKnobs []ParsedKnob

	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return nil, err
		}

		settingValue, err := knobConfig.GetSettingValue()
		if err != nil {
			return nil, fmt.Errorf("failed to get setting value for %s: %w", knobConfig.Name, err)
		}

		parsedKnobs = append(parsedKnobs, ParsedKnob{
			Name:         knobConfig.Name,
			SettingValue: settingValue,
		})
	}

	return parsedKnobs, nil
}

// normalizeValue normalizes configuration values for comparison
// Converts values to bytes for memory/size units to enable proper comparison
// e.g., "16MB" and "16384kB" both normalize to the same value in kB
func NormalizeValue(value string) string {
	// Remove whitespace
	value = strings.TrimSpace(value)
	value = strings.ToUpper(value)

	// Try to parse memory/size units and convert to kB for comparison
	// This handles: kB, MB, GB, TB
	if before, ok := strings.CutSuffix(value, "TB"); ok {
		if num, err := strconv.ParseFloat(before, 64); err == nil {
			return fmt.Sprintf("%.0fKB", num*1024*1024*1024)
		}
	}
	if before, ok := strings.CutSuffix(value, "GB"); ok {
		if num, err := strconv.ParseFloat(before, 64); err == nil {
			return fmt.Sprintf("%.0fKB", num*1024*1024)
		}
	}
	if before, ok := strings.CutSuffix(value, "MB"); ok {
		if num, err := strconv.ParseFloat(before, 64); err == nil {
			return fmt.Sprintf("%.0fKB", num*1024)
		}
	}

	// Already in kB, just return uppercase version
	if strings.HasSuffix(value, "KB") {
		return value
	}

	// Try time units: ms, s, min, h, d
	if before, ok := strings.CutSuffix(value, "D"); ok {
		if num, err := strconv.ParseFloat(before, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*24*60*60*1000)
		}
	}
	if before, ok := strings.CutSuffix(value, "H"); ok {
		if num, err := strconv.ParseFloat(before, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*60*60*1000)
		}
	}
	if before, ok := strings.CutSuffix(value, "MIN"); ok {
		if num, err := strconv.ParseFloat(before, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*60*1000)
		}
	}

	// Already in ms
	if strings.HasSuffix(value, "MS") {
		return value
	}

	if before, ok := strings.CutSuffix(value, "S"); ok {
		if num, err := strconv.ParseFloat(before, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*1000)
		}
	}

	// Return as-is for values without units or unrecognized units
	return value
}
