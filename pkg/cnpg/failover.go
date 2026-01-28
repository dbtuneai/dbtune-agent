package cnpg

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/kubernetes"
)

const (
	// MinTimeSinceFailover is the minimum time to wait after detecting a failover
	// before checking cluster health. This prevents rapid successive checks
	// immediately after failover detection.
	MinTimeSinceFailover = 30 * time.Second
)

// FailoverDetectedError is returned when a failover is detected during tuning.
// This signals that the tuning session should terminate gracefully.
type FailoverDetectedError struct {
	OldPrimary string
	NewPrimary string
	Message    string
}

func (e *FailoverDetectedError) Error() string {
	return fmt.Sprintf("failover detected: %s -> %s: %s", e.OldPrimary, e.NewPrimary, e.Message)
}

// isClusterHealthy checks if the CNPG cluster is in a healthy, stable state.
// Returns true if cluster is ready for configuration changes.
func isClusterHealthy(clusterStatus *kubernetes.CNPGClusterStatus) bool {
	// Check 1: All instances must be ready
	if clusterStatus.ReadyInstances != clusterStatus.Instances {
		return false
	}

	// Check 2: Must have a current primary
	if clusterStatus.CurrentPrimary == "" {
		return false
	}

	// Check 3: Target primary must match current primary (no failover in progress)
	if clusterStatus.TargetPrimary != "" && clusterStatus.TargetPrimary != clusterStatus.CurrentPrimary {
		return false
	}

	// Check 4: Phase must indicate healthy state
	// Known healthy phases: "Cluster in healthy state", "Cluster is Ready"
	// Known unhealthy phases: "Failing over", "Switchover in progress", "Waiting for instances to become active"
	unhealthyPhases := []string{
		"Failing over",
		"Switchover in progress",
		"Waiting for instances to become active",
		"Upgrading cluster",
		"Applying configuration",
	}

	phaseLower := strings.ToLower(clusterStatus.Phase)
	for _, unhealthy := range unhealthyPhases {
		if strings.Contains(phaseLower, strings.ToLower(unhealthy)) {
			return false
		}
	}

	// If all checks pass, cluster is healthy
	return true
}

// CheckForFailover checks if a failover has occurred since last check.
// Returns FailoverDetectedError if primary changed, nil otherwise.
func (adapter *CNPGAdapter) CheckForFailover(ctx context.Context) error {
	logger := adapter.Logger()

	clusterName, err := adapter.getClusterName(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster name: %w", err)
	}

	// Get cluster status to check current primary
	clusterStatus, err := adapter.K8sClient.GetCNPGClusterStatus(ctx, clusterName)
	lastKnownPrimary := adapter.State.GetLastKnownPrimary()

	// If we can't get cluster status AND we previously had a primary tracked,
	// this likely means failover is in progress
	if err != nil {
		if lastKnownPrimary != "" {
			logger.Warnf("Cannot get cluster status (failover likely in progress): %v", err)
			return &FailoverDetectedError{
				OldPrimary: lastKnownPrimary,
				NewPrimary: "(unknown - failover in progress)",
				Message:    fmt.Sprintf("cluster status unavailable: %v", err),
			}
		}
		// First check and can't get status - just log and skip
		logger.Warnf("Failed to get cluster status for initial check: %v", err)
		return nil
	}

	currentPrimary := clusterStatus.CurrentPrimary

	// If a failover was recently detected, check if cluster is fully recovered
	// This prevents config applications while cluster is still stabilizing (pg_rewind, WAL replay, etc.)
	timeSinceFailover := adapter.State.TimeSinceLastFailover()
	logger.Debugf("[FAILOVER_RECOVERY] CheckForFailover: currentPrimary=%s, lastKnownPrimary=%s, timeSinceFailover=%v",
		currentPrimary, lastKnownPrimary, timeSinceFailover)
	if timeSinceFailover > 0 {
		logger.Infof("[FAILOVER_RECOVERY] In recovery period after failover (elapsed: %v)", timeSinceFailover.Round(time.Second))
		logger.Infof("[FAILOVER_RECOVERY] Cluster status: ready=%d/%d, primary=%s, target=%s, phase=%s",
			clusterStatus.ReadyInstances, clusterStatus.Instances,
			clusterStatus.CurrentPrimary, clusterStatus.TargetPrimary, clusterStatus.Phase)

		// First check: Is cluster healthy?
		if !isClusterHealthy(clusterStatus) {
			logger.Infof("[FAILOVER_RECOVERY] Waiting for cluster health (ready=%d/%d, phase=%s)",
				clusterStatus.ReadyInstances, clusterStatus.Instances, clusterStatus.Phase)
			return &FailoverDetectedError{
				OldPrimary: lastKnownPrimary,
				NewPrimary: "(recovery in progress)",
				Message: fmt.Sprintf("cluster not healthy: ready=%d/%d, phase=%s, target=%s",
					clusterStatus.ReadyInstances, clusterStatus.Instances,
					clusterStatus.Phase, clusterStatus.TargetPrimary),
			}
		}

		// Second check: Has minimum stabilization time passed?
		if timeSinceFailover < MinTimeSinceFailover {
			remainingTime := MinTimeSinceFailover - timeSinceFailover
			logger.Infof("[FAILOVER_RECOVERY] Waiting for stabilization period (%v remaining)", remainingTime.Round(time.Second))
			return &FailoverDetectedError{
				OldPrimary: lastKnownPrimary,
				NewPrimary: "(stabilizing)",
				Message:    fmt.Sprintf("cluster healthy but stabilizing, %v remaining", remainingTime.Round(time.Second)),
			}
		}

		// Both conditions met: cluster is healthy AND stabilization period passed
		logger.Infof("[FAILOVER_RECOVERY] ALLOWING - cluster healthy and stable after %v", timeSinceFailover.Round(time.Second))

		// Check if primary changed during recovery - this is a NEW failover
		// Restart the recovery timer immediately
		if currentPrimary != lastKnownPrimary {
			logger.Warnf("[FAILOVER_RECOVERY] PRIMARY CHANGED DURING RECOVERY: %s → %s", lastKnownPrimary, currentPrimary)
			logger.Warnf("[FAILOVER_RECOVERY] Treating as new failover, restarting recovery period")

			// Update tracked primary and restart recovery timer
			adapter.State.SetLastKnownPrimary(currentPrimary)
			adapter.State.SetLastFailoverTime(time.Now())

			// Return error to continue blocking operations
			return &FailoverDetectedError{
				OldPrimary: lastKnownPrimary,
				NewPrimary: currentPrimary,
				Message:    "primary changed during recovery, restarting stabilization",
			}
		}

		// Primary unchanged and cluster stable - verify PostgreSQL is actually accepting connections
		// CNPG relies on Kubernetes readiness probes, but PostgreSQL might need additional time
		// to finalize promotion and be ready for configuration changes
		if err := adapter.PGDriver.Ping(ctx); err != nil {
			logger.Infof("[FAILOVER_RECOVERY] Waiting for PostgreSQL to accept connections: %v", err)
			return &FailoverDetectedError{
				OldPrimary: lastKnownPrimary,
				NewPrimary: "(PostgreSQL unavailable)",
				Message:    fmt.Sprintf("cluster healthy but PostgreSQL not ready: %v", err),
			}
		}

		// All checks passed - clear recovery period and resume operations
		adapter.State.SetLastFailoverTime(time.Time{})
		adapter.State.CreateOperationsContext() // Create new context for operations to proceed
		logger.Infof("[FAILOVER_RECOVERY] Recovery complete, PostgreSQL accepting connections, resuming normal operations")
		return nil
	}

	// If cluster reports no current primary AND we had one before, failover is happening
	if currentPrimary == "" && lastKnownPrimary != "" {
		logger.Warnf("Cluster has no current primary (failover in progress, was: %s)", lastKnownPrimary)
		return &FailoverDetectedError{
			OldPrimary: lastKnownPrimary,
			NewPrimary: "(none - failover in progress)",
			Message:    fmt.Sprintf("cluster phase: %s", clusterStatus.Phase),
		}
	}

	// First time checking - just record current primary
	if lastKnownPrimary == "" {
		if currentPrimary != "" {
			adapter.State.SetLastKnownPrimary(currentPrimary)
			logger.Infof("Tracking primary pod: %s", currentPrimary)
		}
		return nil
	}

	// Check if primary changed (failover occurred)
	if currentPrimary != lastKnownPrimary {
		logger.Warnf("[FAILOVER_RECOVERY] PRIMARY CHANGED: %s → %s", lastKnownPrimary, currentPrimary)
		logger.Warnf("[FAILOVER_RECOVERY] Failover detected at phase=%s, ready=%d/%d",
			clusterStatus.Phase, clusterStatus.ReadyInstances, clusterStatus.Instances)

		return &FailoverDetectedError{
			OldPrimary: lastKnownPrimary,
			NewPrimary: currentPrimary,
			Message:    fmt.Sprintf("cluster phase: %s", clusterStatus.Phase),
		}
	}

	// Check if failover is in progress (target primary is being changed)
	if clusterStatus.TargetPrimary != "" && clusterStatus.TargetPrimary != currentPrimary {
		logger.Errorf("Failover detected (target primary changed): %s → %s (phase: %s)",
			currentPrimary, clusterStatus.TargetPrimary, clusterStatus.Phase)

		return &FailoverDetectedError{
			OldPrimary: lastKnownPrimary,
			NewPrimary: clusterStatus.TargetPrimary,
			Message:    fmt.Sprintf("primary changing, phase: %s", clusterStatus.Phase),
		}
	}

	// No failover detected
	return nil
}

// HandleFailoverDetected is called when a failover is detected.
// It sends an error to the backend and updates tracking to the new primary.
func (adapter *CNPGAdapter) HandleFailoverDetected(ctx context.Context, failoverErr *FailoverDetectedError) error {
	logger := adapter.Logger()

	logger.Errorf("Failover detected: %s", failoverErr.Error())
	logger.Info("Sending failover notification to backend")

	// Record failover time to track cluster recovery
	// CheckForFailover will verify cluster health before allowing config applications
	// Only set this for new failover detections, not for recovery status checks
	if failoverErr.NewPrimary != "(recovery in progress)" {
		adapter.State.SetLastFailoverTime(time.Now())
		logger.Info("Failover detected - will monitor cluster health before allowing config applications")

		// Cancel any in-flight PostgreSQL operations immediately
		// This prevents errors from operations that started before failover was detected
		adapter.State.CancelOperations()
	}

	// Send failover error to backend
	// Backend decides: stop tuning session? apply baseline? etc.
	errorPayload := agent.ErrorPayload{
		ErrorMessage: failoverErr.Error(),
		ErrorType:    "failover_detected",
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	}
	adapter.SendError(errorPayload)

	// Update tracking to new primary
	// IMPORTANT: Only update if NewPrimary is a valid pod name, not a placeholder
	if failoverErr.NewPrimary != "" && !containsPlaceholder(failoverErr.NewPrimary) {
		adapter.State.SetLastKnownPrimary(failoverErr.NewPrimary)
		logger.Infof("Updated tracked primary: %s", failoverErr.NewPrimary)
	} else {
		logger.Infof("Not updating tracked primary - value is placeholder or transient state: %q (keeping: %s)",
			failoverErr.NewPrimary, failoverErr.OldPrimary)
	}

	// Return error to signal caller that failover occurred
	return failoverErr
}

// containsPlaceholder checks if a string is a placeholder value (not a real pod name)
func containsPlaceholder(s string) bool {
	// Check for parenthesis-based placeholders
	if len(s) > 0 && s[0] == '(' {
		return true
	}

	// Check for CNPG cluster states that are not actual pod names
	invalidStates := []string{"pending", "none", "unknown", ""}
	for _, state := range invalidStates {
		if s == state {
			return true
		}
	}

	return false
}
