package cloudsql

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/tuning"
	"google.golang.org/api/sqladmin/v1"
)

// ApplyConfig applies the proposed configuration to the CloudSQL instance.
//
// We cannot validate trivially against the CloudSQL API which parameters
// require a restart, so we rely on the KnobApplication signal provided.
// CloudSQL's flag API does not surface a per-flag restart-required hint
// post-application, so no mismatch detection is possible here.
func (adapter *CloudSQLAdapter) ApplyConfig(ctx context.Context, proposedConfig *tuning.ProposedConfigResponse) tuning.ApplyConfigError {
	adapter.Logger().Infof("Applying config")

	if proposedConfig.KnobApplication == tuning.KnobApplicationRestart && !config.IsRestartAllowed() {
		return &tuning.RestartNotAllowedError{
			Message: "restart is not allowed in the agent",
		}
	}

	flags := []*sqladmin.DatabaseFlags{}

	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := tuning.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to find recommended knob: %w", err)}
		}
		fmtValue, err := knobConfig.GetSettingValue()
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to get setting value: %w", err)}
		}

		param := &sqladmin.DatabaseFlags{
			Name:  knobConfig.Name,
			Value: fmtValue,
		}

		flags = append(flags, param)
	}

	err := adapter.CloudSQLAdminClient.ApplyFlags(adapter.CloudSQLConfig.ProjectID, adapter.CloudSQLConfig.DatabaseName, flags)
	if err != nil {
		return &tuning.ConfigApplyError{Err: err}
	}

	err = tuning.WaitPostgresReady(adapter.PGDriver, ctx)
	if err != nil {
		return &tuning.ConfigApplyError{Err: fmt.Errorf("Error waiting for PostgreSQL to come back online: %w", err)}
	}
	return nil
}

func (adapter *CloudSQLAdapter) GetActiveConfig(ctx context.Context) (tuning.ConfigArraySchema, error) {
	adapter.Logger().Debugf("Getting Active Config")

	config, err := tuning.GetActiveConfig(adapter.PGDriver, ctx)
	if err != nil {
		return nil, err
	}

	// some config rows are marked as internal, we cannot control these at all and
	// they change in a way we can't predict and so can cause tuning to fail
	filteredConfig := make(tuning.ConfigArraySchema, 0, len(config))

	for _, knob := range config {
		if k, ok := knob.(tuning.PGConfigRow); ok {
			if k.Context != "internal" {
				filteredConfig = append(filteredConfig, k)
			} else {
				adapter.Logger().Debugf("Internal config option filtered out: %s", k.Name)
			}
		} else {
			adapter.Logger().Errorf("Unexpected Config Type: %T", knob)
		}
	}

	return filteredConfig, nil
}
