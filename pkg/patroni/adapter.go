package patroni

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
)

type PatroniAdapter struct {
	agent.CommonAgent
	PatroniConfig   Config
	PGDriver        *pgxpool.Pool
	GuardrailConfig guardrails.Config
	pgConfig        pg.Config
	PGVersion       string
	HTTPClient      *http.Client
	State           *State
}

func CreatePatroniAdapter() (*PatroniAdapter, error) {
	ctx := context.Background()

	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	patroniConfig, err := ConfigFromViper()
	if err != nil {
		return nil, err
	}

	pgPool, err := pgxpool.New(ctx, pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	pgVersion, err := pg.PGVersion(pgPool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}

	guardrailConfig, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails: %w", err)
	}

	common := agent.CreateCommonAgent()
	adpt := PatroniAdapter{
		CommonAgent:     *common,
		PatroniConfig:   patroniConfig,
		PGDriver:        pgPool,
		GuardrailConfig: guardrailConfig,
		pgConfig:        pgConfig,
		PGVersion:       pgVersion,
		HTTPClient:      &http.Client{Timeout: 60 * time.Second}, // Increased for restart operations
		State:           &State{},
	}

	adpt.InitCollectors(adpt.Collectors())

	// Initialize operations context for PostgreSQL queries
	// This context will be cancelled during failover to abort in-flight operations
	adpt.State.CreateOperationsContext()
	common.Logger().Info("Initialized operations context for PostgreSQL queries")

	// Initialize primary tracking at startup to establish baseline
	// This prevents false failover detection on first ApplyConfig call
	common.Logger().Info("Initializing primary node tracking...")
	if err := adpt.initializePrimaryTracking(ctx); err != nil {
		common.Logger().Warnf("Failed to initialize primary tracking: %v", err)
		// Don't fail agent startup - tracking will be established on first check
	}

	return &adpt, nil
}

// initializePrimaryTracking establishes the initial primary node tracking
// This should be called once during adapter initialization
func (adapter *PatroniAdapter) initializePrimaryTracking(ctx context.Context) error {
	logger := adapter.Logger()

	clusterStatus, err := adapter.getPatroniClusterStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get initial cluster status: %w", err)
	}

	if clusterStatus.CurrentPrimary != "" {
		adapter.State.SetLastKnownPrimary(clusterStatus.CurrentPrimary)
		logger.Infof("Initial primary node tracked: %s", clusterStatus.CurrentPrimary)
	} else {
		logger.Warn("No primary node detected during initialization")
	}

	return nil
}

func (adapter *PatroniAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	// Use operations context that will be cancelled during failover
	// This ensures queries during config application are aborted if failover occurs
	ctx := adapter.State.GetOperationsContext()
	logger := adapter.Logger()

	logger.Infof("[FAILOVER_RECOVERY] ApplyConfig called: knob_application=%s, num_params=%d",
		proposedConfig.KnobApplication, len(proposedConfig.Config))

	// Check for failover before applying new configuration
	// This ensures we don't apply tuning parameters after a failover has occurred
	if err := adapter.CheckForFailover(ctx); err != nil {
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Warnf("[FAILOVER_RECOVERY] Failover check BLOCKED config application: %s", failoverErr.Message)
			// Return error to block this config application (matches CNPG behavior)
			// HandleFailoverDetected was already called by CheckForFailover
			return failoverErr
		}
		// Other error checking failover status - log but continue
		logger.Warnf("Failed to check for failover: %v", err)
	}

	logger.Infof("[FAILOVER_RECOVERY] Failover check PASSED - proceeding with config application")

	// Parse and validate all knobs upfront (following CNPG pattern)
	parsedKnobs, err := parameters.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return fmt.Errorf("failed to parse knob configurations: %w", err)
	}

	if len(parsedKnobs) == 0 {
		logger.Info("No configuration changes to apply")
		return nil
	}

	// Step 1: Clear postgresql.auto.conf by resetting each parameter that will be changed
	// This ensures no local postgresql.auto.conf settings override Patroni DCS settings
	logger.Info("Clearing postgresql.auto.conf by resetting parameters...")
	for _, knob := range parsedKnobs {
		// Skip resetting Patroni-managed parameters
		if IsPatroniManagedParameter(knob.Name) {
			logger.Warnf("Skipping reset for Patroni-managed parameter: %s", knob.Name)
			continue
		}
		logger.Infof("Resetting parameter: %s", knob.Name)
		err := pg.AlterSystemReset(adapter.PGDriver, knob.Name)
		if err != nil {
			logger.Errorf("Failed to reset parameter %s: %v", knob.Name, err)
			return fmt.Errorf("failed to reset parameter %s: %w", knob.Name, err)
		}
		logger.Infof("Successfully reset parameter: %s", knob.Name)
	}

	// Reload configuration after reset
	logger.Info("Reloading PostgreSQL configuration after reset...")
	err = pg.ReloadConfig(adapter.PGDriver)
	if err != nil {
		logger.Errorf("Failed to reload configuration: %v", err)
		return fmt.Errorf("failed to reload configuration after reset: %w", err)
	}

	// Step 2: Build parameters map for Patroni REST API
	pgParameters := make(map[string]interface{})
	for _, knob := range parsedKnobs {
		if IsPatroniManagedParameter(knob.Name) {
			logger.Warnf("Skipping Patroni-managed parameter: %s (value: %s)", knob.Name, knob.SettingValue)
			continue
		}
		logger.Infof("Will set %s = %s", knob.Name, knob.SettingValue)
		pgParameters[knob.Name] = knob.SettingValue
	}

	// Build the JSON payload in Patroni's expected format
	patchRequest := PatroniPatchRequest{}
	patchRequest.PostgreSQL.Parameters = pgParameters

	// Convert to JSON
	jsonData, err := json.Marshal(patchRequest)
	if err != nil {
		logger.Errorf("Error marshaling JSON: %v", err)
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	logger.Debugf("Patroni PATCH payload: %s", string(jsonData))

	// Step 3: Apply configuration via Patroni REST API
	configURL := fmt.Sprintf("%s/config", adapter.PatroniConfig.PatroniAPIURL)

	// Create HTTP PATCH request with context
	// This allows the request to be cancelled if failover occurs
	req, err := http.NewRequestWithContext(ctx, "PATCH", configURL, bytes.NewBuffer(jsonData))
	if err != nil {
		logger.Errorf("Error creating request: %v", err)
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set Content-Type header
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("Error sending request: %v", err)
		return fmt.Errorf("failed to send HTTP PATCH request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Errorf("Error reading response: %v", err)
		return fmt.Errorf("failed to read response body: %w", err)
	}

	logger.Infof("Patroni API Response Status: %s", resp.Status)
	logger.Debugf("Patroni API Response Body: %s", string(responseBody))

	// Check if the request was successful (2xx status codes)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Errorf("Patroni API returned error status: %d, body: %s", resp.StatusCode, string(responseBody))
		return fmt.Errorf("patroni API returned error status %d: %s", resp.StatusCode, string(responseBody))
	}

	logger.Info("Configuration successfully applied via Patroni REST API")

	// Step 4: Check if any parameters require PostgreSQL restart and handle restart if needed
	// Check both: explicit restart flag OR dynamic detection of restart-required parameters
	needsRestart := proposedConfig.KnobApplication == "restart"

	// If not explicitly flagged as restart, check dynamically using pg_settings
	if !needsRestart {
		// Build list of parameter names to check
		var paramNames []string
		for _, knob := range parsedKnobs {
			if !IsPatroniManagedParameter(knob.Name) {
				paramNames = append(paramNames, knob.Name)
			}
		}

		logger.Infof("Checking if any of %d parameters require restart: %v", len(paramNames), paramNames)

		// Check if any parameter requires restart (context='postmaster')
		restartRequired, err := adapter.requiresPostgreSQLRestart(ctx, paramNames)
		if err != nil {
			logger.Errorf("Failed to check for restart-required parameters: %v", err)
			// Continue without restart check if query fails
		} else if restartRequired {
			needsRestart = true
			logger.Warn("Detected parameters that require PostgreSQL restart (context='postmaster')")
		} else {
			logger.Info("No restart-required parameters detected")
		}
	}

	// If restart is needed, trigger PostgreSQL restart via Patroni API
	if needsRestart {
		logger.Warn("Configuration requires restart, triggering PostgreSQL restart via Patroni API...")

		// Mark that we're entering an intentional restart window
		// This prevents CheckForFailover from sending failover notifications during planned restart
		adapter.State.SetInRestartWindow(true)
		defer adapter.State.SetInRestartWindow(false) // Clear flag when done (success or failure)

		// Trigger the restart
		err = adapter.triggerPostgreSQLRestart(ctx)
		if err != nil {
			logger.Errorf("Failed to trigger PostgreSQL restart: %v", err)
			return fmt.Errorf("failed to trigger PostgreSQL restart: %w", err)
		}

		// Wait for PostgreSQL to come back online
		logger.Warn("Waiting for PostgreSQL to be ready after restart...")
		err = pg.WaitPostgresReady(adapter.PGDriver)
		if err != nil {
			// Check if error indicates failover during restart
			if isPostgreSQLFailoverError(err) {
				logger.Warnf("PostgreSQL failover detected during restart wait: %v", err)

				// Send failover notification if this is first detection
				if adapter.State.TimeSinceLastFailover() == 0 {
					adapter.State.SetLastFailoverTime(time.Now())
					errorPayload := agent.ErrorPayload{
						ErrorMessage: fmt.Sprintf("failover detected: PostgreSQL restart interrupted by failover: %v", err),
						ErrorType:    "failover_detected",
						Timestamp:    time.Now().UTC().Format(time.RFC3339),
					}
					adapter.SendError(errorPayload)
					logger.Info("Failover notification sent to backend (during restart wait)")
				}

				return &FailoverDetectedError{
					OldPrimary: adapter.State.GetLastKnownPrimary(),
					NewPrimary: NoPrimaryDetected,
					Message:    fmt.Sprintf("PostgreSQL restart interrupted by failover: %v", err),
				}
			}
			return fmt.Errorf("failed to wait for PostgreSQL to be back online: %w", err)
		}
		logger.Info("PostgreSQL is ready after restart")

		// Create a new operations context after successful restart
		// The old context may have been used during restart and needs to be refreshed
		adapter.State.CreateOperationsContext()
		ctx = adapter.State.GetOperationsContext()
		logger.Info("Created new operations context after PostgreSQL restart")
	}

	// Step 5: Verify that all parameters were applied correctly by reading pg_settings
	logger.Info("Waiting for Patroni to apply configuration (15s delay)...")
	time.Sleep(15 * time.Second)
	logger.Info("Verifying applied configuration in pg_settings...")
	for _, knob := range parsedKnobs {
		// Skip verification for Patroni-managed parameters
		if IsPatroniManagedParameter(knob.Name) {
			logger.Warnf("Skipping verification for Patroni-managed parameter: %s", knob.Name)
			continue
		}

		query := `SELECT setting, unit FROM pg_settings WHERE name = $1`
		var actualSetting string
		var unit *string
		err := utils.QueryRowWithPrefix(adapter.PGDriver, ctx, query, knob.Name).Scan(&actualSetting, &unit)
		if err != nil {
			// Check if error indicates PostgreSQL failover (before Patroni detects it)
			if isPostgreSQLFailoverError(err) {
				logger.Warnf("PostgreSQL failover detected during verification: %v", err)

				// Send failover notification if this is first detection
				if adapter.State.TimeSinceLastFailover() == 0 {
					adapter.State.SetLastFailoverTime(time.Now())
					errorPayload := agent.ErrorPayload{
						ErrorMessage: fmt.Sprintf("failover detected: PostgreSQL shutting down: %v", err),
						ErrorType:    "failover_detected",
						Timestamp:    time.Now().UTC().Format(time.RFC3339),
					}
					adapter.SendError(errorPayload)
					logger.Info("Failover notification sent to backend (during verification)")
				}

				return &FailoverDetectedError{
					OldPrimary: adapter.State.GetLastKnownPrimary(),
					NewPrimary: NoPrimaryDetected,
					Message:    fmt.Sprintf("PostgreSQL shutting down: %v", err),
				}
			}
			logger.Errorf("Failed to verify parameter %s: %v", knob.Name, err)
			return fmt.Errorf("failed to verify parameter %s in pg_settings: %w", knob.Name, err)
		}

		// Reconstruct the actual value with unit if present
		actualValue := actualSetting
		if unit != nil && *unit != "" {
			actualValue = actualSetting + *unit
		}

		expectedValue := knob.SettingValue

		// Try to match expected value with actual value
		// This handles both exact matches and unit variations
		if actualValue == expectedValue || actualSetting == expectedValue {
			logger.Infof("✓ Verified parameter %s = %s", knob.Name, actualValue)
		} else if normalizeValue(actualValue) == normalizeValue(expectedValue) {
			// Values match after normalization (e.g., "16384kB" vs "16MB")
			logger.Infof("✓ Verified parameter %s = %s (matches expected %s)",
				knob.Name, actualValue, expectedValue)
		} else {
			// True mismatch - this is a critical error
			logger.Errorf("Parameter verification FAILED for %s: expected '%s', but got '%s' from pg_settings",
				knob.Name, expectedValue, actualValue)
			return fmt.Errorf("configuration verification failed: parameter %s has value '%s' but expected '%s'",
				knob.Name, actualValue, expectedValue)
		}
	}

	logger.Info("Configuration application and verification complete")

	// Final failover check after applying configuration
	if err := adapter.CheckForFailover(ctx); err != nil {
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			// Failover detected after config application
			// CheckForFailover already sent notification and updated LastKnownPrimary
			return failoverErr
		}
		logger.Warnf("Post-application failover check encountered error: %v", err)
	}

	return nil
}

// requiresPostgreSQLRestart checks if any of the parameters being applied require a PostgreSQL restart
// by querying pg_settings to see if context='postmaster' for those parameters
func (adapter *PatroniAdapter) requiresPostgreSQLRestart(ctx context.Context, parameterNames []string) (bool, error) {
	logger := adapter.Logger()

	if len(parameterNames) == 0 {
		return false, nil
	}

	// Query pg_settings to check if any parameter has context='postmaster'
	// context='postmaster' means the parameter requires a server restart
	query := `SELECT name FROM pg_settings WHERE name = ANY($1) AND context = 'postmaster'`

	rows, err := utils.QueryWithPrefix(adapter.PGDriver, ctx, query, parameterNames)
	if err != nil {
		return false, fmt.Errorf("failed to query pg_settings for restart-required parameters: %w", err)
	}
	defer rows.Close()

	var restartRequiredParams []string
	for rows.Next() {
		var paramName string
		if err := rows.Scan(&paramName); err != nil {
			return false, fmt.Errorf("failed to scan parameter name: %w", err)
		}
		restartRequiredParams = append(restartRequiredParams, paramName)
	}

	if len(restartRequiredParams) > 0 {
		logger.Infof("Parameters requiring PostgreSQL restart: %v", restartRequiredParams)
		return true, nil
	}

	return false, nil
}

// triggerPostgreSQLRestart triggers a PostgreSQL restart for all cluster nodes
// This ensures the entire cluster is restarted when restart-required parameters are applied
// Works for both tuning iterations and baseline revert after failover
func (adapter *PatroniAdapter) triggerPostgreSQLRestart(ctx context.Context) error {
	logger := adapter.Logger()

	logger.Warn("Triggering PostgreSQL restart for all cluster nodes...")

	// Get full cluster information including all members
	clusterURL := fmt.Sprintf("%s/cluster", adapter.PatroniConfig.PatroniAPIURL)
	req, err := http.NewRequestWithContext(ctx, "GET", clusterURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create cluster info request: %w", err)
	}

	resp, err := adapter.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get cluster info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cluster API returned non-200 status: %s", resp.Status)
	}

	var clusterInfo PatroniClusterResponse
	if err := json.NewDecoder(resp.Body).Decode(&clusterInfo); err != nil {
		return fmt.Errorf("failed to decode cluster info: %w", err)
	}

	logger.Infof("Found %d nodes in cluster to restart", len(clusterInfo.Members))

	// Strategy: Restart replicas first, then primary last
	// This minimizes downtime and avoids unnecessary failover
	var replicas []PatroniMember
	var leader *PatroniMember

	for i, member := range clusterInfo.Members {
		if member.Role == "leader" || member.Role == "master" {
			leader = &clusterInfo.Members[i]
			logger.Infof("Found leader: %s (host: %s)", member.Name, member.Host)
		} else {
			replicas = append(replicas, member)
			logger.Infof("Found replica: %s (host: %s, role: %s)", member.Name, member.Host, member.Role)
		}
	}

	// Restart replicas first (safe, no downtime)
	// Wait between restarts to avoid "restart already in progress" errors
	for i, replica := range replicas {
		logger.Infof("Restarting replica %d/%d: %s", i+1, len(replicas), replica.Name)
		if err := adapter.restartSingleNode(ctx, replica); err != nil {
			// Check if restart already in progress - this is OK, means Patroni is already handling it
			if strings.Contains(err.Error(), "restart already in progress") ||
			   strings.Contains(err.Error(), "reinitialize already in progress") {
				logger.Infof("Restart already in progress for replica %s (Patroni handling it)", replica.Name)
			} else {
				logger.Errorf("Failed to restart replica %s: %v", replica.Name, err)
			}
			// Continue with other nodes even if one fails
		} else {
			logger.Infof("Successfully initiated restart for replica: %s", replica.Name)
		}

		// Wait 3 seconds between replica restarts to avoid conflicts
		if i < len(replicas)-1 {
			logger.Debugf("Waiting 3s before restarting next replica...")
			time.Sleep(3 * time.Second)
		}
	}

	// Wait before restarting leader to ensure replicas are being handled
	if len(replicas) > 0 && leader != nil {
		logger.Info("Waiting 5s for replica restarts to stabilize before restarting leader...")
		time.Sleep(5 * time.Second)
	}

	// Restart leader last (this may cause brief connection interruption)
	if leader != nil {
		logger.Infof("Restarting leader: %s", leader.Name)
		if err := adapter.restartSingleNode(ctx, *leader); err != nil {
			// Check if restart already in progress - this is OK
			if strings.Contains(err.Error(), "restart already in progress") ||
			   strings.Contains(err.Error(), "reinitialize already in progress") {
				logger.Infof("Restart already in progress for leader %s (Patroni handling it)", leader.Name)
			} else {
				logger.Errorf("Failed to restart leader %s: %v", leader.Name, err)
				return fmt.Errorf("failed to restart leader: %w", err)
			}
		} else {
			logger.Infof("Successfully initiated restart for leader: %s", leader.Name)
		}
	}

	logger.Info("PostgreSQL restart initiated for all cluster nodes")
	return nil
}

// restartSingleNode restarts a single node via its Patroni API
func (adapter *PatroniAdapter) restartSingleNode(ctx context.Context, member PatroniMember) error {
	logger := adapter.Logger()

	// Build the restart URL for this specific member
	// Patroni API typically runs on port 8008 on each node
	// Use the member's host and assume same port as configured
	memberRestartURL := fmt.Sprintf("http://%s:8008/restart", member.Host)

	logger.Debugf("Sending POST to Patroni restart endpoint: %s for node %s", memberRestartURL, member.Name)

	req, err := http.NewRequestWithContext(ctx, "POST", memberRestartURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create restart request: %w", err)
	}

	resp, err := adapter.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send restart request: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read restart response: %w", err)
	}

	logger.Debugf("Restart API response for %s: status=%s, body=%s", member.Name, resp.Status, string(responseBody))

	// Patroni returns 202 Accepted for successful restart initiation
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("patroni restart API returned error status %d: %s", resp.StatusCode, string(responseBody))
	}

	return nil
}

// PatroniPatchRequest represents the structure for PATCH request to Patroni's /config endpoint
type PatroniPatchRequest struct {
	PostgreSQL struct {
		Parameters map[string]interface{} `json:"parameters"`
	} `json:"postgresql"`
}

// normalizeValue normalizes configuration values for comparison
// Converts values to bytes for memory/size units to enable proper comparison
// e.g., "16MB" and "16384kB" both normalize to the same value in kB
func normalizeValue(value string) string {
	// Remove whitespace
	value = strings.TrimSpace(value)
	value = strings.ToUpper(value)

	// Try to parse memory/size units and convert to kB for comparison
	// This handles: kB, MB, GB, TB
	if strings.HasSuffix(value, "TB") {
		numStr := strings.TrimSuffix(value, "TB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fKB", num*1024*1024*1024)
		}
	}
	if strings.HasSuffix(value, "GB") {
		numStr := strings.TrimSuffix(value, "GB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fKB", num*1024*1024)
		}
	}
	if strings.HasSuffix(value, "MB") {
		numStr := strings.TrimSuffix(value, "MB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fKB", num*1024)
		}
	}
	if strings.HasSuffix(value, "KB") {
		// Already in kB, just return uppercase version
		return value
	}

	// Try time units: ms, s, min, h, d
	if strings.HasSuffix(value, "D") {
		numStr := strings.TrimSuffix(value, "D")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*24*60*60*1000)
		}
	}
	if strings.HasSuffix(value, "H") {
		numStr := strings.TrimSuffix(value, "H")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*60*60*1000)
		}
	}
	if strings.HasSuffix(value, "MIN") {
		numStr := strings.TrimSuffix(value, "MIN")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*60*1000)
		}
	}
	if strings.HasSuffix(value, "S") && !strings.HasSuffix(value, "MS") {
		numStr := strings.TrimSuffix(value, "S")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*1000)
		}
	}
	if strings.HasSuffix(value, "MS") {
		// Already in ms
		return value
	}

	// Return as-is for values without units or unrecognized units
	return value
}

func (adapter *PatroniAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	logger := adapter.Logger()
	// Use operations context that will be cancelled during failover
	ctx := adapter.State.GetOperationsContext()

	// Check for failover before querying PostgreSQL
	// During recovery, PostgreSQL may be unavailable or unreliable
	if err := adapter.CheckForFailover(ctx); err != nil {
		// Block operation for ANY error from CheckForFailover
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			// Return the failover error (not nil) so runner's isRecoveryError() can skip sending
			// This prevents sending empty config to backend which would stop tuning session
			return nil, failoverErr
		}
		// Non-failover error (cluster status check failed, etc.)
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING GetActiveConfig due to cluster check failure: %v", err)
		return nil, err
	}

	// CRITICAL: The database itself is the only reliable source of truth for the
	// currently active configuration. The Patroni DCS can have stale data or not
	// reflect local overrides in postgresql.auto.conf, so we must query PostgreSQL directly.
	logger.Info("Fetching active configuration from PostgreSQL")
	config, err := pg.GetActiveConfig(adapter.PGDriver, ctx, logger)
	if err != nil {
		// Check if error indicates PostgreSQL failover (before Patroni detects it)
		// This can happen before Patroni cluster status reflects the failover
		if isPostgreSQLFailoverError(err) {
			logger.Warnf("[FAILOVER_RECOVERY] PostgreSQL reports failover in progress: %v", err)

			// Trigger failover detection immediately
			// Don't wait for Patroni cluster status to update (can take 5+ seconds)
			if adapter.State.TimeSinceLastFailover() == 0 {
				// First detection - record failover time and send notification
				adapter.State.SetLastFailoverTime(time.Now())
				logger.Errorf("[FAILOVER_RECOVERY] Failover detected via PostgreSQL error (before Patroni status update)")

				// Send failover notification (don't call HandleFailoverDetected as it would update LastKnownPrimary)
				errorPayload := agent.ErrorPayload{
					ErrorMessage: fmt.Sprintf("Failover detected: %s", err.Error()),
					ErrorType:    "failover_detected",
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				adapter.SendError(errorPayload)
				logger.Info("Failover notification sent to backend")
			}

			// Return FailoverDetectedError so runner's isRecoveryError() can skip sending
			// This prevents sending empty config to backend which would stop tuning session
			logger.Infof("[FAILOVER_RECOVERY] Returning failover error to prevent empty data being sent")
			return nil, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: NoPrimaryDetected,
				Message:    fmt.Sprintf("PostgreSQL error: %v", err),
			}
		}
		// For other errors, return as-is
		return nil, err
	}

	// Filter out Patroni-managed parameters to prevent false "unexpected config change" alerts
	// These parameters are managed by Patroni (e.g., during failover) and should not trigger
	// backend notifications about configuration drift
	filteredConfig := make(agent.ConfigArraySchema, 0, len(config))
	for _, param := range config {
		// Type assert to PGConfigRow to access the Name field
		if row, ok := param.(agent.PGConfigRow); ok {
			if !IsPatroniManagedParameter(row.Name) {
				filteredConfig = append(filteredConfig, param)
			} else {
				adapter.Logger().Debugf("Filtering out Patroni-managed parameter from active config: %s", row.Name)
			}
		} else {
			// If type assertion fails, include the parameter to be safe
			filteredConfig = append(filteredConfig, param)
		}
	}

	return filteredConfig, nil
}

func (adapter *PatroniAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
	logger := adapter.Logger()
	// Use operations context that will be cancelled during failover
	ctx := adapter.State.GetOperationsContext()

	logger.Println("Collecting system info for Patroni cluster")

	// Check for failover before collecting system info
	// During recovery, PostgreSQL may be unavailable
	if err := adapter.CheckForFailover(ctx); err != nil {
		// Block operation for ANY error from CheckForFailover
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			// Return the failover error (not nil) so runner's isRecoveryError() can skip sending
			// This prevents sending empty system info to backend which would stop tuning session
			return nil, failoverErr
		}
		// Non-failover error (cluster status check failed, etc.)
		// Still block to avoid sending errors during unstable cluster state
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING GetSystemInfo due to cluster check failure: %v", err)
		return nil, err
	}

	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		// Check if error indicates PostgreSQL failover
		if isPostgreSQLFailoverError(err) {
			logger.Warnf("[FAILOVER_RECOVERY] PostgreSQL failover detected in GetSystemInfo: %v", err)
			// Record failover and send notification if this is first detection
			if adapter.State.TimeSinceLastFailover() == 0 {
				adapter.State.SetLastFailoverTime(time.Now())
				logger.Errorf("[FAILOVER_RECOVERY] Failover detected via PostgreSQL error in GetSystemInfo")

				// Send failover notification (don't call HandleFailoverDetected)
				errorPayload := agent.ErrorPayload{
					ErrorMessage: fmt.Sprintf("Failover detected: %s", err.Error()),
					ErrorType:    "failover_detected",
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				adapter.SendError(errorPayload)
				logger.Info("Failover notification sent to backend")
			}
			// Return FailoverDetectedError so runner's isRecoveryError() can skip sending
			return nil, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: NoPrimaryDetected,
				Message:    fmt.Sprintf("PostgreSQL error: %v", err),
			}
		}
		return nil, err
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		// Check if error indicates PostgreSQL failover
		if isPostgreSQLFailoverError(err) {
			logger.Warnf("[FAILOVER_RECOVERY] PostgreSQL failover detected in GetSystemInfo: %v", err)
			// Record failover and send notification if this is first detection
			if adapter.State.TimeSinceLastFailover() == 0 {
				adapter.State.SetLastFailoverTime(time.Now())
				logger.Errorf("[FAILOVER_RECOVERY] Failover detected via PostgreSQL error in GetSystemInfo")

				// Send failover notification (don't call HandleFailoverDetected)
				errorPayload := agent.ErrorPayload{
					ErrorMessage: fmt.Sprintf("Failover detected: %s", err.Error()),
					ErrorType:    "failover_detected",
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				adapter.SendError(errorPayload)
				logger.Info("Failover notification sent to backend")
			}
			// Return FailoverDetectedError so runner's isRecoveryError() can skip sending
			return nil, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: NoPrimaryDetected,
				Message:    fmt.Sprintf("PostgreSQL error: %v", err),
			}
		}
		return nil, err
	}

	memoryInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	hostInfo, err := host.Info()
	if err != nil {
		return nil, err
	}

	noCPUs, err := cpu.Counts(true)
	if err != nil {
		return nil, err
	}

	// Convert into metrics
	totalMemory, err := metrics.NodeMemoryTotal.AsFlatValue(memoryInfo.Total)
	if err != nil {
		return nil, err
	}
	version, _ := metrics.PGVersion.AsFlatValue(pgVersion)
	hostOS, _ := metrics.NodeOSInfo.AsFlatValue(hostInfo.OS)
	platform, _ := metrics.NodeOSPlatform.AsFlatValue(hostInfo.Platform)
	platformVersion, _ := metrics.NodeOSPlatformVer.AsFlatValue(hostInfo.PlatformVersion)
	maxConnectionsMetric, _ := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	noCPUsMetric, _ := metrics.NodeCPUCount.AsFlatValue(noCPUs)

	systemInfo := []metrics.FlatValue{
		version,
		totalMemory,
		hostOS,
		platformVersion,
		platform,
		maxConnectionsMetric,
		noCPUsMetric,
	}

	return systemInfo, nil
}

func (adapter *PatroniAdapter) Guardrails() *guardrails.Signal {
	// Get memory info
	memoryInfo, err := mem.VirtualMemory()
	if err != nil {
		adapter.Logger().Error("Failed to get memory info:", err)
		return nil
	}

	// Calculate memory usage percentage
	memoryUsagePercent := float64(memoryInfo.Total-memoryInfo.Available) / float64(memoryInfo.Total) * 100

	adapter.Logger().Debugf("Memory usage: %f%%", memoryUsagePercent)

	// If memory usage is greater than threshold (default 90%), trigger critical guardrail
	if memoryUsagePercent > adapter.GuardrailConfig.MemoryThreshold {
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}

func (adapter *PatroniAdapter) Collectors() []agent.MetricCollector {
	pool := adapter.PGDriver
	collectors := []agent.MetricCollector{
		{
			Key:       "database_average_query_runtime",
			Collector: pg.PGStatStatements(pool, adapter.pgConfig.IncludeQueries, adapter.pgConfig.MaximumQueryTextLength),
		},
		{
			Key:       "database_transactions_per_second",
			Collector: pg.TransactionsPerSecond(pool),
		},

		{
			Key:       "system_db_size",
			Collector: pg.DatabaseSize(pool),
		},
		{
			Key:       "database_autovacuum_count",
			Collector: pg.Autovacuum(pool),
		},
		{
			Key:       "server_uptime",
			Collector: pg.UptimeMinutes(pool),
		},
		{
			Key:       "pg_database",
			Collector: pg.PGStatDatabase(pool),
		},
		{
			Key:       "pg_user_tables",
			Collector: pg.PGStatUserTables(pool),
		},
		{
			Key:       "pg_bgwriter",
			Collector: pg.PGStatBGwriter(pool),
		},
		{
			Key:       "pg_wal",
			Collector: pg.PGStatWAL(pool),
		},
		{
			Key:       "database_wait_events",
			Collector: pg.WaitEvents(pool),
		},
		{
			Key:       "hardware",
			Collector: HardwareInfoPatroni(),
		},
	}

	majorVersion := strings.Split(adapter.PGVersion, ".")
	intMajorVersion, err := strconv.Atoi(majorVersion[0])
	if err != nil {
		adapter.Logger().Warnf("Could not parse major version from version string %s: %v", adapter.PGVersion, err)
		return collectors
	}
	if intMajorVersion >= 17 {
		collectors = append(collectors, agent.MetricCollector{
			Key:       "pg_checkpointer",
			Collector: pg.PGStatCheckpointer(pool),
		})
	}

	return collectors
}
