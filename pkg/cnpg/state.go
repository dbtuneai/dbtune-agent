package cnpg

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type State struct {
	mu sync.RWMutex // Protects all fields from concurrent access

	LastGuardrailCheck time.Time
	LastAppliedConfig  time.Time

	// Failover detection
	// LastKnownPrimary is the primary pod name from the last check
	// Used to detect when primary changes (failover)
	LastKnownPrimary string
	// LastFailoverTime is when the last failover was detected
	// Used to prevent config applications during failover recovery period
	LastFailoverTime time.Time

	// Context cancellation for in-flight operations
	// When failover is detected, cancel this context to abort any in-flight PostgreSQL queries
	operationsCtx    context.Context
	cancelOperations context.CancelFunc
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

// SetLastFailoverTime safely updates the last failover timestamp
func (s *State) SetLastFailoverTime(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.IsZero() {
		log.Infof("[STATE] Clearing LastFailoverTime (was: %v)", s.LastFailoverTime)
	} else {
		log.Infof("[STATE] Setting LastFailoverTime to %v", t)
	}
	s.LastFailoverTime = t
}

// TimeSinceLastFailover safely returns time since last failover
func (s *State) TimeSinceLastFailover() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.LastFailoverTime.IsZero() {
		// No failover has occurred yet
		return time.Duration(0)
	}
	return time.Since(s.LastFailoverTime)
}

// CreateOperationsContext creates a new cancellable context for operations
// Call this when the adapter is initialized and when recovery completes
func (s *State) CreateOperationsContext() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If we already have a valid (not cancelled) context, don't recreate it
	// This prevents concurrent CheckForFailover calls from cancelling each other's contexts
	if s.operationsCtx != nil && s.operationsCtx.Err() == nil {
		log.Debugf("[STATE] Operations context already valid, skipping recreation")
		return
	}

	// Cancel any existing context first (should only be the pre-cancelled recovery context)
	if s.cancelOperations != nil {
		s.cancelOperations()
	}

	// Create new cancellable context
	s.operationsCtx, s.cancelOperations = context.WithCancel(context.Background())
	log.Infof("[STATE] Created new operations context")
}

// GetOperationsContext returns the current operations context
// This context will be cancelled when failover is detected
// During recovery, returns a pre-cancelled context
func (s *State) GetOperationsContext() context.Context {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.operationsCtx == nil {
		// Fallback to Background context (should only happen during initialization)
		return context.Background()
	}
	return s.operationsCtx
}

// CancelOperations cancels any in-flight operations
// Call this when failover is detected to abort PostgreSQL queries immediately
func (s *State) CancelOperations() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancelOperations != nil {
		log.Infof("[STATE] Cancelling in-flight operations due to failover")
		s.cancelOperations()

		// Create a new pre-cancelled context immediately
		// This ensures GetOperationsContext() returns a cancelled context during recovery
		// so any operations that bypass CheckForFailover will still fail fast
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		s.operationsCtx = ctx
		s.cancelOperations = nil // Don't keep the cancel func since it's already called
	}
}
