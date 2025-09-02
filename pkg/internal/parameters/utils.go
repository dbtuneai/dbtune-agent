package parameters

import (
	"fmt"

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
