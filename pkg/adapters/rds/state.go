package rds

import (
	"time"
)

type State struct {
	// LastApplyAttempt is the last apply, successful or not. Debouncing on the
	// attempt keeps a failing apply from retrying every config tick.
	LastApplyAttempt   time.Time
	LastGuardrailCheck time.Time
	LastDBInfoCheck    time.Time
	DBInfo             *DBInfo
}

// CheckApplyDebounced reports whether an apply was attempted within d.
func (s *State) CheckApplyDebounced(d time.Duration) bool {
	return s.LastApplyAttempt.Add(d).After(time.Now())
}
