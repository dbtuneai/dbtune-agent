package parameters

import (
	"fmt"

	"github.com/dbtuneai/agent/pkg/agent"
)

// FindRecommendedKnob finds a recommended knob in recommendation array
func FindRecommendedKnob(config []agent.PGConfigRow, knob string) (agent.PGConfigRow, error) {
	for _, c := range config {
		if c.Name == knob {
			return c, nil
		}
	}
	return agent.PGConfigRow{}, fmt.Errorf("knob %s not found in configuration", knob)
}
