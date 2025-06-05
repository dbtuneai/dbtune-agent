package guardrails

type CriticalityLevel string

const (
	// Critical is a guardrail that is critical to the operation of the database
	// and should be reverted immediately. This also means that the DBtune server
	// will revert to the baseline configuration to stabilise the system before recommending
	// a new configuration.
	Critical CriticalityLevel = "critical"
	// NonCritical is a guardrail that is not critical
	// to the operation of the database, but a new configuration
	// is recommended to be applied.
	NonCritical CriticalityLevel = "non-critical"
)

type Type string

const (
	// Memory is a guardrail that is related to the memory of the database
	Memory         Type = "memory"
	FreeableMemory Type = "freeable_memory"
)

type Signal struct {
	Level CriticalityLevel `json:"level"`
	Type  Type             `json:"type"`
}
