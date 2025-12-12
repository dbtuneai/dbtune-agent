package patroni

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
)

const (
	// NoPrimaryDetected is used as a placeholder when no primary node can be identified
	NoPrimaryDetected = "NO PRIMARY DETECTED"
)

// isPostgreSQLFailoverError checks if an error indicates PostgreSQL failover in progress
// This can detect failovers BEFORE Patroni cluster status updates (which can take 5+ seconds)
func isPostgreSQLFailoverError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "database system is shutting down") ||
		strings.Contains(errStr, "database system is starting up") ||
		strings.Contains(errStr, "terminating connection due to administrator command") ||
		(strings.Contains(errStr, "cannot execute") && strings.Contains(errStr, "recovery"))
}

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
				CurrentPrimary: strings.TrimSpace(member.Name),
				State:          member.State,
			}, nil
		}
	}

	// If no leader is found, it's a critical state (e.g., during failover)
	return &PatroniClusterStatus{CurrentPrimary: "", State: "no-leader"}, nil
}

// isClusterHealthy checks if the Patroni cluster is stable and ready for operations
func (adapter *PatroniAdapter) isClusterHealthy(ctx context.Context) (bool, error) {
	logger := adapter.Logger()

	// Check 1: Get cluster status from Patroni REST API
	clusterStatus, err := adapter.getPatroniClusterStatus(ctx)
	if err != nil {
		logger.Warnf("Cannot get Patroni cluster status: %v", err)
		return false, err
	}

	// Check 2: Ensure cluster has a current primary
	if clusterStatus.CurrentPrimary == "" {
		logger.Warnf("Patroni cluster has no current primary")
		return false, nil
	}

	// Check 3: Ensure cluster state is running (not "Failing over" or other transitional state)
	if clusterStatus.State != "running" && clusterStatus.State != "" {
		logger.Warnf("Patroni cluster not in running state: %s", clusterStatus.State)
		return false, nil
	}

	// Check 4: Verify PostgreSQL is accepting connections
	if err := adapter.PGDriver.Ping(ctx); err != nil {
		if isPostgreSQLFailoverError(err) {
			logger.Warnf("PostgreSQL not accepting connections (failover in progress): %v", err)
			return false, nil
		}
		logger.Warnf("PostgreSQL ping failed: %v", err)
		return false, err
	}

	logger.Debugf("Cluster health check passed: primary=%s, state=%s, PostgreSQL accepting connections",
		clusterStatus.CurrentPrimary, clusterStatus.State)

	return true, nil
}

// CheckForFailover checks if a failover has occurred since last check.
// Returns FailoverDetectedError if primary changed or cluster unhealthy, nil otherwise.
func (adapter *PatroniAdapter) CheckForFailover(ctx context.Context) error {
	logger := adapter.Logger()

	// CRITICAL: During active failover recovery, block ALL operations
	// Check if we're tracking a failover (LastFailoverTime is set)
	timeSinceFailover := adapter.State.TimeSinceLastFailover()
	if timeSinceFailover > 0 {
		// We detected a failover previously - now check if cluster is healthy
		healthy, err := adapter.isClusterHealthy(ctx)
		if err != nil {
			// Can't determine health - keep blocking
			logger.Warnf("[FAILOVER_RECOVERY] Cannot verify cluster health: %v", err)
			return &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: "(verifying health)",
				Message:    fmt.Sprintf("cluster health check failed after %.1fs: %v", timeSinceFailover.Seconds(), err),
			}
		}

		if !healthy {
			// Cluster still unhealthy - keep blocking
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked: cluster still recovering (%.1fs since failover)",
				timeSinceFailover.Seconds())
			return &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: "(recovering)",
				Message:    fmt.Sprintf("cluster still recovering: %.1fs since failover", timeSinceFailover.Seconds()),
			}
		}

		// Cluster is healthy now - clear failover state and allow operations
		logger.Infof("[FAILOVER_RECOVERY] Cluster healthy again (%.1fs since failover) - resuming normal operations",
			timeSinceFailover.Seconds())
		adapter.State.ClearFailoverTime()
		// Continue to check if primary changed below
	}

	// Get cluster status from Patroni REST API
	clusterStatus, err := adapter.getPatroniClusterStatus(ctx)
	lastKnownPrimary := adapter.State.GetLastKnownPrimary()

	// If we can't get cluster status AND we previously had a primary tracked,
	// this likely means failover is in progress
	if err != nil {
		if lastKnownPrimary != "" {
			logger.Warnf("Cannot get Patroni cluster status (failover likely in progress): %v", err)

			// Only send notification if this is the FIRST detection (not already in recovery)
			if adapter.State.TimeSinceLastFailover() == 0 {
				adapter.State.SetLastFailoverTime(time.Now())

				// Send failover notification to backend
				errorPayload := agent.ErrorPayload{
					ErrorMessage: fmt.Sprintf("failover detected: %s -> NO PRIMARY DETECTED: cluster status unavailable: %v",
						lastKnownPrimary, err),
					ErrorType:    "failover_detected",
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				adapter.SendError(errorPayload)
				logger.Info("Failover notification sent to backend (cluster status unavailable)")
			}

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

		// Only send notification if this is the FIRST detection (not already in recovery)
		if adapter.State.TimeSinceLastFailover() == 0 {
			adapter.State.SetLastFailoverTime(time.Now())

			// Send failover notification to backend
			errorPayload := agent.ErrorPayload{
				ErrorMessage: fmt.Sprintf("failover detected: %s -> NO PRIMARY DETECTED: cluster state: %s",
					lastKnownPrimary, clusterStatus.State),
				ErrorType:    "failover_detected",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			adapter.SendError(errorPayload)
			logger.Info("Failover notification sent to backend (no current primary)")
		}

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
		logger.Warnf("Primary name comparison: lastKnown='%s' (len=%d), current='%s' (len=%d)",
			lastKnownPrimary, len(lastKnownPrimary), currentPrimary, len(currentPrimary))

		// CRITICAL: Update LastKnownPrimary IMMEDIATELY to prevent detecting same failover again
		// This must happen before returning the error to avoid infinite loop
		adapter.State.SetLastKnownPrimary(currentPrimary)

		// Record failover time to trigger recovery period
		adapter.State.SetLastFailoverTime(time.Now())
		logger.Errorf("[FAILOVER_RECOVERY] NEW failover detected - cluster entering recovery state")

		// Send failover notification to backend
		errorPayload := agent.ErrorPayload{
			ErrorMessage: fmt.Sprintf("failover detected: %s -> %s: cluster state: %s",
				lastKnownPrimary, currentPrimary, clusterStatus.State),
			ErrorType:    "failover_detected",
			Timestamp:    time.Now().UTC().Format(time.RFC3339),
		}
		adapter.SendError(errorPayload)
		logger.Info("Failover notification sent to backend")

		return &FailoverDetectedError{
			OldPrimary: lastKnownPrimary,
			NewPrimary: currentPrimary,
			Message:    fmt.Sprintf("cluster state: %s", clusterStatus.State),
		}
	}

	logger.Debugf("Failover check passed: primary unchanged (%s)", currentPrimary)

	return nil
}
