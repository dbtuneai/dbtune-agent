package runner

import (
	"reflect"

	"github.com/dbtuneai/agent/pkg/agent"
)

// Runner is the main entry point for the agent
// that executes the different tasks
// It now uses the new channel-based architecture
func Runner(adapter agent.AgentLooper) {
	// Extract the CommonAgent from the adapter
	// All adapters embed agent.CommonAgent as a field
	commonAgent := extractCommonAgent(adapter)
	if commonAgent == nil {
		panic("Unable to extract CommonAgent from adapter")
	}

	// Use the new runner
	RunnerNew(commonAgent, adapter)
}

// extractCommonAgent uses reflection to extract the embedded CommonAgent field
func extractCommonAgent(adapter agent.AgentLooper) *agent.CommonAgent {
	v := reflect.ValueOf(adapter)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// Try to find a field named "CommonAgent"
	if v.Kind() == reflect.Struct {
		field := v.FieldByName("CommonAgent")
		if field.IsValid() {
			return field.Addr().Interface().(*agent.CommonAgent)
		}
	}

	return nil
}
