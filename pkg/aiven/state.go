package aiven

import "time"

// Aiven interfaces
type Hardware struct {
	TotalMemoryBytes int64
	NumCPUs          int
	LastChecked      time.Time
}

type State struct {
	Hardware                       Hardware
	InitialSharedBuffersPercentage float64
	InitialWorkMem                 int64
	LastAppliedConfig              time.Time
	// HACK: Used to trigger restarts on ALTER DATABASE statements
	LastKnownPGStatMonitorEnable bool
	// Guardrails
	LastGuardrailCheck            time.Time
	LastMemoryAvailableTime       time.Time
	LastMemoryAvailablePercentage float64
	LastHardwareInfoTime          time.Time
}
