package parameters

import (
	"errors"
	"example.com/dbtune-agent/internal/utils"
	"fmt"
)

// FindRecommendedKnob finds a recommended knob in recommendation array
func FindRecommendedKnob(config []utils.PGConfigRow, knob string) (utils.PGConfigRow, error) {
	for _, c := range config {
		if c.Name == knob {
			return c, nil
		}
	}
	return utils.PGConfigRow{}, errors.New(fmt.Sprintf("Knob %s not found in configuration", knob))
}
