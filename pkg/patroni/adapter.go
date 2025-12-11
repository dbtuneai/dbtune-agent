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
		HTTPClient:      &http.Client{Timeout: 10 * time.Second},
		State:           &State{},
	}

	adpt.InitCollectors(adpt.Collectors())

	// Capture baseline configuration from Patroni DCS at startup
	// This will be used to revert configuration after failover
	common.Logger().Info("Capturing baseline configuration from Patroni DCS...")
	if err := adpt.CaptureBaselineConfiguration(ctx); err != nil {
		common.Logger().Warnf("Failed to capture baseline configuration: %v", err)
		// Don't fail agent startup, but baseline revert after failover won't work
	} else {
		common.Logger().Info("Baseline configuration captured successfully")
	}

	return &adpt, nil
}

func (adapter *PatroniAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	ctx := context.Background()
	logger := adapter.Logger()
	logger.Infof("Applying config via Patroni REST API: %s", proposedConfig.KnobApplication)

	// Check for failover before applying configuration
	if err := adapter.CheckForFailover(ctx); err != nil {
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			// Failover detected - handle it and abort config application
			return adapter.HandleFailoverDetected(ctx, failoverErr)
		}
		// Other error - log and continue (non-critical)
		logger.Warnf("Failover check encountered error (continuing): %v", err)
	}

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

	// Create HTTP PATCH request
	req, err := http.NewRequest("PATCH", configURL, bytes.NewBuffer(jsonData))
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

	// Step 4: Wait for PostgreSQL if restart is needed
	if proposedConfig.KnobApplication == "restart" {
		logger.Warn("Configuration requires restart, waiting for PostgreSQL to be ready...")
		err = pg.WaitPostgresReady(adapter.PGDriver)
		if err != nil {
			return fmt.Errorf("failed to wait for PostgreSQL to be back online: %w", err)
		}
		logger.Info("PostgreSQL is ready after restart")
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
		err := adapter.PGDriver.QueryRow(ctx, query, knob.Name).Scan(&actualSetting, &unit)
		if err != nil {
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
			return adapter.HandleFailoverDetected(ctx, failoverErr)
		}
		logger.Warnf("Post-application failover check encountered error: %v", err)
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
	// CRITICAL: The database itself is the only reliable source of truth for the
	// currently active configuration. The Patroni DCS can have stale data or not
	// reflect local overrides in postgresql.auto.conf, so we must query PostgreSQL directly.
	adapter.Logger().Info("Fetching active configuration from PostgreSQL")
	config, err := pg.GetActiveConfig(adapter.PGDriver, context.Background(), adapter.Logger())
	if err != nil {
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
	adapter.Logger().Println("Collecting system info for Patroni cluster")

	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
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

// CaptureBaselineConfiguration fetches the current configuration from Patroni DCS
// and stores it as the baseline for future revert operations after failover
func (adapter *PatroniAdapter) CaptureBaselineConfiguration(ctx context.Context) error {
	logger := adapter.Logger()

	// Fetch current configuration from Patroni DCS
	configURL := fmt.Sprintf("%s/config", adapter.PatroniConfig.PatroniAPIURL)
	req, err := http.NewRequestWithContext(ctx, "GET", configURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create GET request: %w", err)
	}

	resp, err := adapter.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch Patroni config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("patroni API returned non-200 status: %s", resp.Status)
	}

	// Parse the response
	var patroniConfig struct {
		PostgreSQL struct {
			Parameters map[string]interface{} `json:"parameters"`
		} `json:"postgresql"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&patroniConfig); err != nil {
		return fmt.Errorf("failed to decode Patroni config: %w", err)
	}

	// Store the baseline configuration in State
	// Filter out Patroni-managed parameters as we don't tune those
	baselineConfig := make(map[string]interface{})
	for paramName, paramValue := range patroniConfig.PostgreSQL.Parameters {
		if !IsPatroniManagedParameter(paramName) {
			baselineConfig[paramName] = paramValue
			logger.Debugf("Captured baseline: %s = %v", paramName, paramValue)
		}
	}

	adapter.State.SetBaselineConfig(baselineConfig)
	logger.Infof("Captured baseline configuration with %d parameters", len(baselineConfig))

	return nil
}

// RevertToBaselineAfterFailover reverts all Patroni DCS configuration to baseline
// This is called automatically after failover detection to ensure a clean, known state
func (adapter *PatroniAdapter) RevertToBaselineAfterFailover(ctx context.Context) error {
	logger := adapter.Logger()
	logger.Info("Reverting Patroni configuration to baseline after failover...")

	// Step 1: Clear postgresql.auto.conf to remove any stale local settings
	logger.Info("Clearing postgresql.auto.conf on new primary...")
	if err := adapter.ClearPostgreSQLAutoConf(ctx); err != nil {
		logger.Errorf("Failed to clear postgresql.auto.conf: %v", err)
		return fmt.Errorf("failed to clear postgresql.auto.conf: %w", err)
	}

	// Step 2: Retrieve the baseline configuration that was captured at startup
	baselineConfig := adapter.State.GetBaselineConfig()
	if len(baselineConfig) == 0 {
		logger.Warn("No baseline configuration found - was it captured at startup?")
		logger.Info("Skipping configuration revert, only cleared postgresql.auto.conf")
		return nil
	}

	logger.Infof("Reverting %d parameters to their baseline values in Patroni DCS...", len(baselineConfig))

	// Step 3: Build the patch request with baseline values
	patchRequest := PatroniPatchRequest{}
	patchRequest.PostgreSQL.Parameters = make(map[string]interface{})

	// Apply each baseline parameter value
	for paramName, paramValue := range baselineConfig {
		patchRequest.PostgreSQL.Parameters[paramName] = paramValue
		logger.Debugf("Reverting parameter in DCS: %s = %v", paramName, paramValue)
	}

	// Convert to JSON
	jsonData, err := json.Marshal(patchRequest)
	if err != nil {
		logger.Errorf("Error marshaling baseline JSON: %v", err)
		return fmt.Errorf("failed to marshal baseline JSON: %w", err)
	}

	logger.Debugf("Patroni baseline PATCH payload: %s", string(jsonData))

	// Step 4: Apply baseline configuration via Patroni REST API
	configURL := fmt.Sprintf("%s/config", adapter.PatroniConfig.PatroniAPIURL)
	req, err := http.NewRequest("PATCH", configURL, bytes.NewBuffer(jsonData))
	if err != nil {
		logger.Errorf("Error creating baseline request: %v", err)
		return fmt.Errorf("failed to create baseline HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("Error sending baseline request: %v", err)
		return fmt.Errorf("failed to send baseline HTTP PATCH request: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Errorf("Error reading baseline response: %v", err)
		return fmt.Errorf("failed to read baseline response body: %w", err)
	}

	logger.Infof("Patroni baseline API Response Status: %s", resp.Status)
	logger.Debugf("Patroni baseline API Response Body: %s", string(responseBody))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Errorf("Patroni API returned error status for baseline: %d, body: %s", resp.StatusCode, string(responseBody))
		return fmt.Errorf("patroni baseline API returned error status %d: %s", resp.StatusCode, string(responseBody))
	}

	logger.Info("✓ Successfully reverted Patroni configuration to baseline after failover")
	logger.Infof("Restored %d parameters to their original baseline values in Patroni DCS", len(baselineConfig))

	return nil
}

// ClearPostgreSQLAutoConf clears all parameters from postgresql.auto.conf
// This is used after failover to ensure the new primary has a clean state
func (adapter *PatroniAdapter) ClearPostgreSQLAutoConf(ctx context.Context) error {
	logger := adapter.Logger()

	// Query pg_file_settings to find all parameters currently in postgresql.auto.conf
	query := `
		SELECT name
		FROM pg_file_settings
		WHERE sourcefile LIKE '%postgresql.auto.conf'
	`

	rows, err := adapter.PGDriver.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query pg_file_settings: %w", err)
	}
	defer rows.Close()

	var parametersToReset []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			logger.Warnf("Failed to scan parameter name: %v", err)
			continue
		}
		parametersToReset = append(parametersToReset, name)
	}

	if len(parametersToReset) == 0 {
		logger.Info("postgresql.auto.conf is already empty, no parameters to reset")
		return nil
	}

	logger.Infof("Found %d parameters in postgresql.auto.conf to reset", len(parametersToReset))

	// Reset each parameter to clear postgresql.auto.conf
	for _, paramName := range parametersToReset {
		// Skip Patroni-managed parameters - these should never be in postgresql.auto.conf anyway
		if IsPatroniManagedParameter(paramName) {
			logger.Warnf("Skipping reset of Patroni-managed parameter: %s", paramName)
			continue
		}

		logger.Infof("Resetting parameter: %s", paramName)
		if err := pg.AlterSystemReset(adapter.PGDriver, paramName); err != nil {
			logger.Errorf("Failed to reset parameter %s: %v", paramName, err)
			// Continue with other parameters even if one fails
		}
	}

	// Reload configuration to apply the resets
	logger.Info("Reloading PostgreSQL configuration after clearing postgresql.auto.conf...")
	if err := pg.ReloadConfig(adapter.PGDriver); err != nil {
		return fmt.Errorf("failed to reload configuration: %w", err)
	}

	logger.Info("Successfully cleared postgresql.auto.conf")
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
