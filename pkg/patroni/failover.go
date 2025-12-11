package patroni

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/pg"
)

const (
	// NoPrimaryDetected is used as a placeholder when no primary node can be identified
	NoPrimaryDetected = "NO PRIMARY DETECTED"
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

// PatroniClusterStatus holds the essential state from the Patroni API.
type PatroniClusterStatus struct {
	CurrentPrimary string
	State          string
}

// PatroniMember represents a member node in the Patroni cluster.
type PatroniMember struct {
	Name  string `json:"name"`
	Role  string `json:"role"`
	Host  string `json:"host"`
	State string `json:"state"`
}

// PatroniClusterResponse represents the Patroni API response structure for the /cluster endpoint.
type PatroniClusterResponse struct {
	Members []PatroniMember `json:"members"`
}

// getPatroniClusterStatus queries the Patroni REST API to get the current cluster leader.
func (adapter *PatroniAdapter) getPatroniClusterStatus(ctx context.Context) (*PatroniClusterStatus, error) {

	// Create a new request with context
	req, err := http.NewRequestWithContext(ctx, "GET", adapter.PatroniConfig.PatroniAPIURL+"/cluster", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Patroni API request: %w", err)
	}

	resp, err := adapter.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call Patroni API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("patroni API returned non-200 status: %s", resp.Status)
	}

	var clusterResponse PatroniClusterResponse
	if err := json.NewDecoder(resp.Body).Decode(&clusterResponse); err != nil {
		return nil, fmt.Errorf("failed to decode Patroni API response: %w", err)
	}

	// Find the leader in the members list
	for _, member := range clusterResponse.Members {
		if member.Role == "leader" {
			return &PatroniClusterStatus{
				CurrentPrimary: member.Name,
				State:          member.State,
			}, nil
		}
	}

	// If no leader is found, it's a critical state (e.g., during failover)
	return &PatroniClusterStatus{CurrentPrimary: "", State: "no-leader"}, nil
}

// CheckForFailover checks if a failover has occurred since last check.
// Returns FailoverDetectedError if primary changed, nil otherwise.
func (adapter *PatroniAdapter) CheckForFailover(ctx context.Context) error {
	logger := adapter.Logger()

	// Get cluster status from Patroni REST API
	clusterStatus, err := adapter.getPatroniClusterStatus(ctx)
	lastKnownPrimary := adapter.State.GetLastKnownPrimary()

	// If we can't get cluster status AND we previously had a primary tracked,
	// this likely means failover is in progress
	if err != nil {
		if lastKnownPrimary != "" {
			logger.Warnf("Cannot get Patroni cluster status (failover likely in progress): %v", err)
			return &FailoverDetectedError{
				OldPrimary: lastKnownPrimary,
				NewPrimary: NoPrimaryDetected,
				Message:    fmt.Sprintf("cluster status unavailable: %v", err),
			}
		}
		// First check and can't get status - just log and skip
		logger.Warnf("Failed to get Patroni cluster status for initial check: %v", err)
		return nil
	}

	currentPrimary := clusterStatus.CurrentPrimary

	// If cluster reports no current primary AND we had one before, failover is happening
	if currentPrimary == "" && lastKnownPrimary != "" {
		logger.Warnf("Patroni cluster has no current primary (failover in progress, was: %s)", lastKnownPrimary)
		return &FailoverDetectedError{
			OldPrimary: lastKnownPrimary,
			NewPrimary: NoPrimaryDetected,
			Message:    fmt.Sprintf("cluster state: %s", clusterStatus.State),
		}
	}

	// First time checking - just record current primary
	if lastKnownPrimary == "" {
		if currentPrimary != "" {
			adapter.State.SetLastKnownPrimary(currentPrimary)
			logger.Infof("Tracking primary node: %s", currentPrimary)
		}
		return nil
	}

	// Check if primary changed (failover occurred)
	if currentPrimary != lastKnownPrimary {
		logger.Warnf("Failover detected: %s → %s (state: %s)",
			lastKnownPrimary, currentPrimary, clusterStatus.State)

		return &FailoverDetectedError{
			OldPrimary: lastKnownPrimary,
			NewPrimary: currentPrimary,
			Message:    fmt.Sprintf("cluster state: %s", clusterStatus.State),
		}
	}

	return nil
}

// HandleFailoverDetected is called when a failover is detected.
// It sends an error to the backend and updates tracking to the new primary.
func (adapter *PatroniAdapter) HandleFailoverDetected(ctx context.Context, failoverErr *FailoverDetectedError) error {
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
	// IMPORTANT: Only update if NewPrimary is a valid node name, not a placeholder
	if failoverErr.NewPrimary != "" && failoverErr.NewPrimary != NoPrimaryDetected {
		adapter.State.SetLastKnownPrimary(failoverErr.NewPrimary)
		logger.Infof("Updated tracked primary: %s", failoverErr.NewPrimary)

		// Wait for the new primary to be ready before attempting recovery actions
		logger.Info("Waiting for new primary to be ready after failover...")
		if err := pg.WaitPostgresReady(adapter.PGDriver); err != nil {
			logger.Errorf("Failed to wait for new primary to be ready: %v", err)
			// Don't return error - failover handling should continue
		} else {
			logger.Info("New primary is ready")
		}
	} else {
		logger.Infof("Not updating tracked primary - failover still in progress (NewPrimary=%s)", failoverErr.NewPrimary)
	}

	// Return error to signal caller that failover occurred
	return failoverErr
}
