package cnpg

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
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
		logger.Warnf("Failover detected: %s → %s (phase: %s)",
			lastKnownPrimary, currentPrimary, clusterStatus.Phase)

		return &FailoverDetectedError{
			OldPrimary: lastKnownPrimary,
			NewPrimary: currentPrimary,
			Message:    fmt.Sprintf("cluster phase: %s", clusterStatus.Phase),
		}
	}

	// Check if failover is in progress in two ways:
	// 1. targetPrimary != currentPrimary (primary being changed)
	// 2. phase indicates failover/switchover (even if target==current during transition)
	failoverPhases := []string{"Failing over", "Switchover in progress"}
	isFailoverPhase := false
	for _, fp := range failoverPhases {
		if clusterStatus.Phase == fp {
			isFailoverPhase = true
			break
		}
	}

	if clusterStatus.TargetPrimary != "" && clusterStatus.TargetPrimary != currentPrimary {
		logger.Errorf("Failover detected (target primary changed): %s → %s (phase: %s)",
			currentPrimary, clusterStatus.TargetPrimary, clusterStatus.Phase)

		return &FailoverDetectedError{
			OldPrimary: lastKnownPrimary,
			NewPrimary: clusterStatus.TargetPrimary,
			Message:    fmt.Sprintf("primary changing, phase: %s", clusterStatus.Phase),
		}
	}

	// IMPORTANT: Only trigger if primary actually changed
	// Prevents duplicate detection when cluster stays in "Failing over" phase after we already handled it
	if isFailoverPhase && lastKnownPrimary != "" && currentPrimary != lastKnownPrimary {
		logger.Errorf("Failover detected (cluster in failover phase): phase=%s, primary changed %s → %s",
			clusterStatus.Phase, lastKnownPrimary, currentPrimary)

		return &FailoverDetectedError{
			OldPrimary: lastKnownPrimary,
			NewPrimary: currentPrimary,
			Message:    fmt.Sprintf("cluster in failover phase: %s", clusterStatus.Phase),
		}
	}

	return nil
}

// HandleFailoverDetected is called when a failover is detected.
// It sends an error to the backend and updates tracking to the new primary.
// The backend decides whether to stop a tuning session and apply baseline.
func (adapter *CNPGAdapter) HandleFailoverDetected(ctx context.Context, failoverErr *FailoverDetectedError) error {
	logger := adapter.Logger()

	logger.Errorf("Failover detected: %s", failoverErr.Error())
	logger.Info("Sending failover notification to backend")

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
		logger.Infof("Not updating tracked primary - failover still in progress (NewPrimary=%s)", failoverErr.NewPrimary)
	}

	// Return error to signal caller that failover occurred
	return failoverErr
}

// containsPlaceholder checks if a string is a placeholder value (not a real pod name)
func containsPlaceholder(s string) bool {
	placeholders := []string{"(unknown", "(none", "(", ")"}
	for _, p := range placeholders {
		if len(s) > 0 && (s[0] == '(' || len(s) >= len(p) && s[:len(p)] == p) {
			return true
		}
	}
	return false
}
