package aiven

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aiven/go-client-codegen/handler/service"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/tuning"
)

// ApplyConfig applies configuration changes to the Aiven PostgreSQL service
func (adapter *AivenPostgreSQLAdapter) ApplyConfig(ctx context.Context, proposedConfig *tuning.ProposedConfigResponse) tuning.ApplyConfigError {
	adapter.Logger().Infof("Applying config")

	// List of knobs to be applied
	userConfig := make(map[string]any)
	pgSettings := make(map[string]any)
	restartRequired := false

	// Sort the knobs into dbLevelKnobs and serviceLevelKnobs
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := tuning.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to find recommended knob: %w", err)}
		}

		adapter.Logger().Debugf("Knob config: %+v", knobConfig)

		knobModifiability, ok := aivenModifiableParams[knobConfig.Name]
		if !ok {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("parameter %s has unknown modifiability status on Aiven. Skipping on applying the configuration", knobConfig.Name)}
		}
		switch knobModifiability.ModifyLevel {
		case ModifyUserPGConfig:
			pgSettings[knobConfig.Name] = knobConfig.Setting
		case ModifyServiceLevel:
			// If it requires a restart, it will be automatically handled.
			userConfig[knobConfig.Name] = knobConfig.Setting
			if knobModifiability.RequiresRestart {
				restartRequired = true
			}
		case NoModify:
			return &tuning.ConfigApplyError{Err: fmt.Errorf("parameter %s can not be modified on Aiven. Skipping on applying the configuration", knobConfig.Name)}
		}
	}

	// If any of the parameters being applied would require a restart but the
	// agent is not allowed to restart, bail before pushing changes to Aiven.
	if restartRequired && !config.IsRestartAllowed() {
		return &tuning.RestartNotAllowedError{
			Message: "restart is not allowed in the agent",
		}
	}
	// Refuse to partially apply: KnobApplication=reload but Aiven would
	// auto-restart for at least one of the proposed knobs.
	if restartRequired && proposedConfig.KnobApplication == tuning.KnobApplicationReload {
		return &tuning.ConfigApplyError{Err: fmt.Errorf("refusing to apply: KnobApplication=reload but at least one parameter would force an Aiven service restart")}
	}

	if len(pgSettings) > 0 {
		userConfig["pg"] = pgSettings
	}
	if len(userConfig) > 0 {
		adapter.Logger().Debugf("User config to be sent to Aiven: %+v", userConfig)

		// Apply the configuration update
		_, err := adapter.Client.ServiceUpdate(
			ctx,
			adapter.Config.ProjectName,
			adapter.Config.ServiceName,
			&service.ServiceUpdateIn{UserConfig: &userConfig, Powered: boolPtr(true)},
		)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to update PostgreSQL parameters: %w", err)}
		}
	} else {
		adapter.Logger().Warnf("ApplyConfig was called with no changes to apply")
	}

	if restartRequired {
		adapter.Logger().Info("Restart was required, checking if Aiven PostgreSQL service has restarted")
		err := adapter.waitForServiceState(service.ServiceStateTypeRunning)
		if err != nil {
			return &tuning.ConfigApplyError{Err: err}
		}
	}

	adapter.State.LastAppliedConfig = time.Now()
	return nil
}

// waitForServiceState waits for the service to reach the specified state
func (adapter *AivenPostgreSQLAdapter) waitForServiceState(state service.ServiceStateType) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for service to reach state %s", state)
		case <-time.After(10 * time.Second):
			serviceResponse, err := adapter.Client.ServiceGet(
				context.Background(),
				adapter.Config.ProjectName,
				adapter.Config.ServiceName,
			)
			if err != nil {
				adapter.Logger().Warnf("Failed to get service status: %v", err)
				continue
			}

			if serviceResponse.State == state {
				adapter.Logger().Infof("Service reached state: %s", state)
				return nil
			}
			adapter.Logger().Debugf("Service state: %s, waiting for: %s", serviceResponse.State, state)
		}
	}
}

func boolPtr(b bool) *bool {
	return &b
}

// GetActiveConfig returns the active configuration for the Aiven API
// as well as through PostgreSQL
func (adapter *AivenPostgreSQLAdapter) GetActiveConfig(ctx context.Context) (tuning.ConfigArraySchema, error) {
	// Main differences from the generic tuning.GetActiveConfig:
	// * We inject `shared_buffers_percentage` from the Aiven API.
	//   Until we modify it, Aiven returns nothing, so we fall back to a known default of 20%.
	// * `work_mem` is set through the Aiven API in MB, but pg_settings reports it in kB.
	//   We convert kB→MB and override the unit/context so the value round-trips correctly.
	logger := adapter.Logger()
	logger.Debug("Getting active config for Aiven PostgreSQL")

	service, err := adapter.Client.ServiceGet(
		ctx,
		adapter.Config.ProjectName,
		adapter.Config.ServiceName,
		[2]string{"include_secrets", "false"},
	)
	if err != nil {
		return nil, err
	}

	userConfig := service.UserConfig

	// HACK: This isn't for configRows, it's purely to keep track of
	// this parameter, which we use to trigger session restarts.
	// There's no point re-querying the service for this information
	// elsewhere so we do it here.
	if val, ok := userConfig["pg_stat_monitor_enable"]; ok && val != nil {
		if b, ok := val.(bool); ok {
			adapter.State.LastKnownPGStatMonitorEnable = b
		}
	}

	// Get shared_buffers_percentage from Aiven API, defaulting to 20% if unset.
	var sharedBuffersPercentage float64
	if val, ok := userConfig["shared_buffers_percentage"]; ok && val != nil {
		if f, ok := val.(float64); ok {
			sharedBuffersPercentage = f
		} else {
			return nil, fmt.Errorf("shared_buffers_percentage is not convertable to float64! %v", val)
		}
	} else {
		sharedBuffersPercentage = DEFAULT_SHARED_BUFFERS_PERCENTAGE
	}

	// Get the standard pg_settings config.
	configRows, err := tuning.GetActiveConfig(adapter.PGDriver, ctx)
	if err != nil {
		return nil, err
	}

	// Override work_mem: convert kB (from pg_settings) → MB (Aiven's unit).
	configRows, err = overrideWorkMemKBtoMB(configRows)
	if err != nil {
		return nil, err
	}

	// Prepend the synthetic shared_buffers_percentage setting from Aiven API.
	configRows = prependSharedBuffersPercentage(configRows, sharedBuffersPercentage)

	return configRows, nil
}

// overrideWorkMemKBtoMB finds work_mem in configRows and converts its value
// from kB (as reported by pg_settings) to MB (as used by the Aiven API).
func overrideWorkMemKBtoMB(configRows tuning.ConfigArraySchema) (tuning.ConfigArraySchema, error) {
	for i := 0; i < len(configRows); i++ {
		pgRow, ok := configRows[i].(tuning.PGConfigRow)
		if !ok || pgRow.Name != "work_mem" {
			continue
		}
		var workMemKb int64
		switch v := pgRow.Setting.(type) {
		case int64:
			workMemKb = v
		case string:
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error converting work_mem to int64: %w", err)
			}
			workMemKb = parsed
		default:
			return nil, fmt.Errorf("unexpected work_mem type: %T", v)
		}
		configRows[i] = tuning.PGConfigRow{
			Name:    "work_mem",
			Setting: int(workMemKb / 1024),
			Unit:    "MB",
			Vartype: "integer",
			Context: "service",
		}
		break
	}
	return configRows, nil
}

// prependSharedBuffersPercentage prepends a synthetic shared_buffers_percentage
// setting to the config rows.
func prependSharedBuffersPercentage(configRows tuning.ConfigArraySchema, pct float64) tuning.ConfigArraySchema {
	return append(tuning.ConfigArraySchema{tuning.PGConfigRow{
		Name:    "shared_buffers_percentage",
		Setting: pct,
		Unit:    "percentage",
		Vartype: "real",
		Context: "service",
	}}, configRows...)
}
