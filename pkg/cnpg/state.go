package cnpg

import (
	"sync"
	"time"
)

type State struct {
	mu sync.RWMutex // Protects all fields from concurrent access

	LastGuardrailCheck time.Time
	LastAppliedConfig  time.Time

	// Failover detection fields
	// InitialPrimaryPod is the primary pod name when tuning session started
	InitialPrimaryPod string
	// TuningSessionActive indicates if we're currently in a tuning session
	TuningSessionActive bool
}

// IsTuningActive safely reads the TuningSessionActive flag
func (s *State) IsTuningActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TuningSessionActive
}

// GetInitialPrimary safely reads the InitialPrimaryPod
func (s *State) GetInitialPrimary() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.InitialPrimaryPod
}

// SetTuningSession safely updates tuning session state
func (s *State) SetTuningSession(active bool, primaryPod string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TuningSessionActive = active
	s.InitialPrimaryPod = primaryPod
}

// DeactivateTuningSession safely marks the tuning session as inactive
func (s *State) DeactivateTuningSession() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TuningSessionActive = false
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
