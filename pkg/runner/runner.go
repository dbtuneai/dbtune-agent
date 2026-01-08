package runner

import (
	"github.com/dbtuneai/agent/pkg/agent"
)

// Runner is the main entry point for the agent
// that executes the different tasks
// It now uses the new channel-based architecture
func Runner(adapter agent.AgentLooper) {
	// Get the CommonAgent from the adapter
	// All adapters embed agent.CommonAgent and implement the Common() method
	commonAgent := adapter.Common()

	// Use the new runner
	RunnerNew(commonAgent, adapter)
}
