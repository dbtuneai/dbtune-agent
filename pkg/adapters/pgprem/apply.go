package pgprem

import (
	"context"
	"fmt"
	"os/exec"

	"github.com/dbtuneai/agent/pkg/tuning"
)

func (adapter *DefaultPostgreSQLAdapter) GetActiveConfig(ctx context.Context) (tuning.ConfigArraySchema, error) {
	return tuning.GetActiveConfig(adapter.pgDriver, ctx)
}

func (adapter *DefaultPostgreSQLAdapter) ApplyConfig(ctx context.Context, proposedConfig *tuning.ProposedConfigResponse) tuning.ApplyConfigError {
	adapter.Logger().Infof("Applying Config: %s", proposedConfig.KnobApplication)

	parsedKnobs, err := tuning.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return &tuning.ConfigApplyError{Err: err}
	}

	// Validate against the running PostgreSQL before mutating
	// postgresql.auto.conf, so we never half-apply.
	paramNames := make([]string, 0, len(parsedKnobs))
	for _, k := range parsedKnobs {
		paramNames = append(paramNames, k.Name)
	}
	requiresRestart, err := tuning.ValidateRestartPolicy(adapter.pgDriver, ctx, paramNames, proposedConfig.KnobApplication)
	if err != nil {
		return &tuning.ConfigApplyError{Err: err}
	}
	if requiresRestart && adapter.pgConfig.ServiceName == "" && adapter.pgConfig.RestartScriptPath == "" {
		return &tuning.ConfigApplyError{Err: fmt.Errorf("neither service name nor restart script configured, refusing to apply: a restart is required to take effect")}
	}

	for _, knob := range parsedKnobs {
		err = tuning.AlterSystem(adapter.pgDriver, knob.Name, knob.SettingValue)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to alter system for %s: %w", knob.Name, err)}
		}
	}

	if !requiresRestart {
		// Reload database when everything is applied. KnobApplication=restart
		// with no postmaster-context params falls through here too: the intent
		// is treated as a hint, and we avoid a needless restart.
		err := tuning.ReloadConfig(adapter.pgDriver)
		if err != nil {
			return &tuning.ConfigApplyError{Err: err}
		}
		return nil
	}

	// Restart the service
	adapter.Logger().Warn("Restarting service")

	if adapter.pgConfig.RestartScriptPath != "" {
		// Execute the operator-provided restart script directly (no shell
		// interpolation). The path comes from trusted config.
		//
		// Contract: the script MUST signal success with exit code 0 and failure
		// with any non-zero exit code. Output written to stdout/stderr is treated
		// as diagnostic only (logged on failure) and does not affect the
		// success/failure decision.
		cmd := exec.Command(adapter.pgConfig.RestartScriptPath) //nolint:gosec // path is from trusted config
		output, err := cmd.CombinedOutput()
		exitCode := cmd.ProcessState.ExitCode() // -1 if the process never ran
		if err != nil || exitCode != 0 {
			adapter.Logger().Warnf("restart script %s exited with code %d; output: %s",
				adapter.pgConfig.RestartScriptPath, exitCode, string(output))
			return &tuning.ConfigApplyError{Err: fmt.Errorf("restart script %s failed (exit code %d): %w",
				adapter.pgConfig.RestartScriptPath, exitCode, err)}
		}
		adapter.Logger().Warnf("Service restarted via %s (exit code 0).", adapter.pgConfig.RestartScriptPath)
	} else {
		// Execute systemctl restart command if it fails try executing it with sudo
		cmd := exec.Command("systemctl", "restart", adapter.pgConfig.ServiceName) //nolint:gosec // ServiceName is from trusted config
		if err := cmd.Run(); err != nil {
			adapter.Logger().Warnf("failed to restart PostgreSQL service: %v. Trying with sudo...", err)

			sudoCmd := exec.Command("sudo", "systemctl", "restart", adapter.pgConfig.ServiceName) //nolint:gosec // ServiceName is from trusted config
			if sudoErr := sudoCmd.Run(); sudoErr != nil {
				return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to restart PostgreSQL service with sudo: %w", sudoErr)}
			}
			adapter.Logger().Warn("Service restarted using sudo.")
		} else {
			adapter.Logger().Warn("Service restarted.")
		}
	}

	if err := tuning.WaitPostgresReady(adapter.pgDriver, ctx); err != nil {
		return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to wait for PostgreSQL to be back online: %w", err)}
	}
	return nil
}
