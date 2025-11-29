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

// CheckForFailover checks if a failover has occurred since tuning started.
// Returns FailoverDetectedError if failover is detected, nil otherwise.
func (adapter *CNPGAdapter) CheckForFailover(ctx context.Context) error {
	// Skip check if we haven't started tuning yet
	if !adapter.State.IsTuningActive() || adapter.State.GetInitialPrimary() == "" {
		return nil
	}

	logger := adapter.Logger()
	clusterName, err := adapter.getClusterName(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster name: %w", err)
	}

	// Get cluster status to check for failover
	clusterStatus, err := adapter.K8sClient.GetCNPGClusterStatus(ctx, clusterName)
	if err != nil {
		logger.Warnf("Failed to get cluster status for failover check: %v", err)
		// Don't fail tuning just because we can't check status
		return nil
	}

	// Check if current primary differs from initial primary
	initialPrimary := adapter.State.GetInitialPrimary()
	currentPrimary := clusterStatus.CurrentPrimary
	if currentPrimary != initialPrimary {
		logger.Warnf("Failover detected: initial primary=%s, current primary=%s, phase=%s",
			initialPrimary, currentPrimary, clusterStatus.Phase)

		return &FailoverDetectedError{
			OldPrimary: initialPrimary,
			NewPrimary: currentPrimary,
			Message:    fmt.Sprintf("cluster phase: %s", clusterStatus.Phase),
		}
	}

	// Also check if failover is in progress (targetPrimary != currentPrimary)
	if clusterStatus.TargetPrimary != "" && clusterStatus.TargetPrimary != currentPrimary {
		logger.Warnf("Failover in progress: current=%s, target=%s, phase=%s",
			currentPrimary, clusterStatus.TargetPrimary, clusterStatus.Phase)

		return &FailoverDetectedError{
			OldPrimary: currentPrimary,
			NewPrimary: clusterStatus.TargetPrimary,
			Message:    fmt.Sprintf("failover in progress, phase: %s", clusterStatus.Phase),
		}
	}

	return nil
}

// InitializeTuningSession validates cluster health and tracks the initial primary pod.
// This should be called when the first configuration is applied.
func (adapter *CNPGAdapter) InitializeTuningSession(ctx context.Context) error {
	logger := adapter.Logger()

	// Check cluster health before starting tuning session
	clusterName, err := adapter.getClusterName(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster name: %w", err)
	}

	clusterStatus, err := adapter.K8sClient.GetCNPGClusterStatus(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("failed to check cluster status: %w", err)
	}

	// Reject tuning if cluster is not in healthy state
	if clusterStatus.Phase != "Cluster in healthy state" {
		return fmt.Errorf("cannot start tuning: cluster is not healthy (phase: %s)", clusterStatus.Phase)
	}

	// Reject tuning if not all instances are ready
	if clusterStatus.ReadyInstances != clusterStatus.Instances {
		return fmt.Errorf("cannot start tuning: not all instances ready (%d/%d)",
			clusterStatus.ReadyInstances, clusterStatus.Instances)
	}

	// Reject tuning if failover/switchover is in progress
	if clusterStatus.TargetPrimary != "" && clusterStatus.TargetPrimary != clusterStatus.CurrentPrimary {
		return fmt.Errorf("cannot start tuning: failover in progress (current: %s, target: %s)",
			clusterStatus.CurrentPrimary, clusterStatus.TargetPrimary)
	}

	logger.Infof("Cluster health check passed: phase=%s, ready=%d/%d, primary=%s",
		clusterStatus.Phase, clusterStatus.ReadyInstances, clusterStatus.Instances, clusterStatus.CurrentPrimary)

	// Store current primary pod as initial primary for failover detection
	adapter.State.SetTuningSession(true, clusterStatus.CurrentPrimary)

	logger.Infof("Tuning session initialized: primary=%s", clusterStatus.CurrentPrimary)
	return nil
}

// HandleFailoverDetected is called when a failover is detected.
// It sends an error to the backend and returns an error to terminate the tuning session.
// The backend will handle stopping the session and applying baseline configuration.
func (adapter *CNPGAdapter) HandleFailoverDetected(ctx context.Context, failoverErr *FailoverDetectedError) error {
	logger := adapter.Logger()

	logger.Errorf("Failover detected: %s", failoverErr.Error())
	logger.Warn("Tuning session will be terminated. Baseline configuration will be applied.")

	// Send failover error to backend so it can stop session and apply baseline
	errorPayload := agent.ErrorPayload{
		ErrorMessage: failoverErr.Error(),
		ErrorType:    "failover_detected",
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	}
	adapter.SendError(errorPayload)

	// Mark tuning session as inactive
	adapter.State.DeactivateTuningSession()

	// Return the original failover error to terminate the session
	return failoverErr
}
