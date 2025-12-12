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

	// LastFailoverTime tracks when the most recent failover was detected
	// Used to enforce cooldown period before resuming normal operations
	LastFailoverTime time.Time
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

// SetLastFailoverTime safely records when a failover was detected
func (s *State) SetLastFailoverTime(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastFailoverTime = t
}

// TimeSinceLastFailover safely returns duration since last failover
// Returns 0 if no failover has been recorded yet
func (s *State) TimeSinceLastFailover() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.LastFailoverTime.IsZero() {
		return 0
	}
	return time.Since(s.LastFailoverTime)
}

// ClearFailoverTime resets the failover time (used after recovery completes)
func (s *State) ClearFailoverTime() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastFailoverTime = time.Time{}
}
