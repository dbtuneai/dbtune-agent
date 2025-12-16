package patroni

import (
	"context"
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
	// Used to trigger recovery mode and health checking
	LastFailoverTime time.Time

	// InRestartWindow tracks if we're in an intentional PostgreSQL restart
	// Used to suppress failover notifications during planned restarts
	InRestartWindow bool

	// Context cancellation for in-flight operations during failover
	// When failover is detected, we cancel this context to abort any ongoing
	// PostgreSQL queries/operations that would fail due to connection loss
	operationsCtx    context.Context
	operationsCancel context.CancelFunc
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

// CreateOperationsContext creates a new cancellable context for PostgreSQL operations
// Should be called during initialization and after failover recovery completes
func (s *State) CreateOperationsContext() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Cancel any existing context first
	if s.operationsCancel != nil {
		s.operationsCancel()
	}

	// Create new context for operations
	s.operationsCtx, s.operationsCancel = context.WithCancel(context.Background())
}

// GetOperationsContext returns the current operations context
// This context should be used for all PostgreSQL queries/operations
// It will be cancelled when failover is detected, aborting in-flight operations
func (s *State) GetOperationsContext() context.Context {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.operationsCtx == nil {
		// Return background context as fallback (shouldn't happen in normal operation)
		return context.Background()
	}

	return s.operationsCtx
}

// CancelOperations cancels all in-flight PostgreSQL operations
// Called when failover is detected to immediately abort queries to old primary
func (s *State) CancelOperations() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.operationsCancel != nil {
		s.operationsCancel()
		// Don't create a new context yet - CreateOperationsContext will be called
		// after cluster becomes healthy again
	}
}

// SetInRestartWindow marks that we're in an intentional PostgreSQL restart
// This prevents failover notifications from being sent during planned restarts
func (s *State) SetInRestartWindow(inRestart bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.InRestartWindow = inRestart
}

// IsInRestartWindow returns true if we're currently in an intentional restart
func (s *State) IsInRestartWindow() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.InRestartWindow
}
