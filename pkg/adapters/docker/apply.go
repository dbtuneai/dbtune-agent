package docker

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/tuning"
	"github.com/moby/moby/client"
)

func (d *DockerContainerAdapter) GetActiveConfig(ctx context.Context) (tuning.ConfigArraySchema, error) {
	return tuning.GetActiveConfig(d.PGDriver, ctx)
}

func (d *DockerContainerAdapter) ApplyConfig(ctx context.Context, proposedConfig *tuning.ProposedConfigResponse) tuning.ApplyConfigError {
	d.Logger().Infof("Applying Config: %s", proposedConfig.KnobApplication)

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
	requiresRestart, err := tuning.ValidateRestartPolicy(d.PGDriver, ctx, paramNames, proposedConfig.KnobApplication)
	if err != nil {
		return &tuning.ConfigApplyError{Err: err}
	}

	for _, knob := range parsedKnobs {
		err := tuning.AlterSystem(d.PGDriver, knob.Name, knob.SettingValue)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to alter system for %s: %w", knob.Name, err)}
		}
	}

	if requiresRestart {
		// Restart the service
		d.Logger().Warn("Restarting service")

		// Execute docker restart command
		_, err := d.dockerClient.ContainerRestart(ctx, d.Config.ContainerName, client.ContainerRestartOptions{})
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to restart PostgreSQL service: %w", err)}
		}

		err = tuning.WaitPostgresReady(d.PGDriver, ctx)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to wait for PostgreSQL to be back online: %w", err)}
		}
	} else {
		// Reload database when everything is applied
		err := tuning.ReloadConfig(d.PGDriver)
		if err != nil {
			return &tuning.ConfigApplyError{Err: err}
		}
	}

	return nil
}
