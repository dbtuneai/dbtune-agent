package cnpg

import "time"

type State struct {
	LastGuardrailCheck time.Time
	LastAppliedConfig  time.Time
}
