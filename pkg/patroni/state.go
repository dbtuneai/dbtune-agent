package patroni

import (
	"sync"
	"time"
)

type State struct {
	mu sync.RWMutex // Protects all fields from concurrent access

	LastGuardrailCheck time.Time
	LastAppliedConfig  time.Time

	// Failover detection
	// LastKnownPrimary is the primary node name from the last check
	// Used to detect when primary changes (failover)
	LastKnownPrimary string

	// BaselineConfig stores the initial configuration state before tuning begins
	// This is captured at agent startup and used to revert after failover
	BaselineConfig map[string]interface{}
}

// GetLastKnownPrimary safely reads the last known primary node
func (s *State) GetLastKnownPrimary() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastKnownPrimary
}

// SetLastKnownPrimary safely updates the last known primary node
func (s *State) SetLastKnownPrimary(primaryNode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastKnownPrimary = primaryNode
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

// GetBaselineConfig safely reads the baseline configuration
func (s *State) GetBaselineConfig() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Return a copy to prevent external modification
	baseline := make(map[string]interface{}, len(s.BaselineConfig))
	for k, v := range s.BaselineConfig {
		baseline[k] = v
	}
	return baseline
}

// SetBaselineConfig safely updates the baseline configuration
func (s *State) SetBaselineConfig(config map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.BaselineConfig = make(map[string]interface{}, len(config))
	for k, v := range config {
		s.BaselineConfig[k] = v
	}
}
