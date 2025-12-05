package cnpg

import (
	"sync"
	"time"
)

type State struct {
	mu sync.RWMutex // Protects all fields from concurrent access

	LastGuardrailCheck time.Time
	LastAppliedConfig  time.Time

	// Failover detection
	// LastKnownPrimary is the primary pod name from the last check
	// Used to detect when primary changes (failover)
	LastKnownPrimary string
}

// GetLastKnownPrimary safely reads the last known primary pod
func (s *State) GetLastKnownPrimary() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastKnownPrimary
}

// SetLastKnownPrimary safely updates the last known primary pod
func (s *State) SetLastKnownPrimary(primaryPod string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastKnownPrimary = primaryPod
}

// UpdateLastAppliedConfig safely updates the last applied config timestamp
func (s *State) UpdateLastAppliedConfig() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastAppliedConfig = time.Now()
}

// UpdateLastGuardrailCheck safely updates the last guardrail check timestamp
func (s *State) UpdateLastGuardrailCheck() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastGuardrailCheck = time.Now()
}

// TimeSinceLastGuardrailCheck safely returns time since last guardrail check
func (s *State) TimeSinceLastGuardrailCheck() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Since(s.LastGuardrailCheck)
}
