package azureflex

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v5"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/tuning"
	"github.com/sirupsen/logrus"
)

// ApplyConfig applies the proposed configuration to the Azure Flex server.
//
// We cannot validate trivially against the Azure API which parameters require
// a restart, so we rely on the KnobApplication signal provided. If after
// applying we observe a per-parameter restart-required hint that contradicts
// a KnobApplication=reload signal, we return restart_not_allowed once the
// full configuration has been pushed; we do not roll back.
func (adapter *AzureFlexAdapter) ApplyConfig(ctx context.Context, proposedConfig *tuning.ProposedConfigResponse) tuning.ApplyConfigError {
	// Bail before pushing parameter changes to Azure if a restart is required but not allowed.
	if proposedConfig.KnobApplication == tuning.KnobApplicationRestart && !config.IsRestartAllowed() {
		return &tuning.RestartNotAllowedError{
			Message: "restart is not allowed in the agent",
		}
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return &tuning.ConfigApplyError{Err: err}
	}

	clientFactory, err := armpostgresqlflexibleservers.NewClientFactory(adapter.AzureFlexConfig.SubscriptionID, cred, nil)
	if err != nil {
		return &tuning.ConfigApplyError{Err: err}
	}

	paramsClient := clientFactory.NewConfigurationsClient()

	anyShouldRestart := false
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := tuning.FindRecommendedKnob(proposedConfig.Config, knob)
		adapter.Logger().Infof("Applying Knob: %s, %v\n", knob, knobConfig.Setting)
		if err != nil {
			return &tuning.ConfigApplyError{Err: err}
		}
		// Apply parameters one by one; if one fails we bail out early so the
		// wrong config can be detected and reverted to baseline.
		shouldRestart, err := ApplyParameter(ctx, adapter.Logger(), paramsClient, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName, knobConfig)
		if err != nil {
			return &tuning.ConfigApplyError{Err: err}
		}
		if shouldRestart {
			anyShouldRestart = true
		}
	}

	// All parameters have been pushed. If KnobApplication=reload but Azure
	// flagged at least one knob as restart-required, the intent and reality
	// have diverged: surface restart_not_allowed without restarting.
	if proposedConfig.KnobApplication == tuning.KnobApplicationReload && anyShouldRestart {
		return &tuning.RestartNotAllowedError{
			Message: "at least one parameter requires a restart but KnobApplication=reload",
		}
	}

	if proposedConfig.KnobApplication == tuning.KnobApplicationRestart && anyShouldRestart {
		// Restart the service
		adapter.Logger().Warn("Restarting service")
		restartResp, err := clientFactory.NewServersClient().BeginRestart(ctx, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName, nil)
		if err != nil {
			return &tuning.ConfigApplyError{Err: err}
		}

		// wait for restart to complete
		for !(restartResp.Done()) {
			// if the network is flakey, Poll will just sit for ages, this timeout
			// stops that from happening. Since it is a polling loop it will retry
			// anyway. Obviously it could be bad if it retries indefinitely
			// We are basically assuming that at some point we will get a response
			timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			resp, err := restartResp.Poll(timeoutCtx)
			if err != nil {
				fmt.Printf("Error: %v", err)
			} else {
				adapter.Logger().Infof("RestartPoller status: %s", resp.Status)
			}
		}

		_, err = restartResp.Result(ctx)
		if err != nil {
			return &tuning.ConfigApplyError{Err: err}
		}
	}

	return nil
}

func ApplyParameter(ctx context.Context, logger *logrus.Logger, paramsClient *armpostgresqlflexibleservers.ConfigurationsClient, resourceGroupName string, serverName string, config tuning.PGConfigRow) (bool, error) {
	value, err := config.GetSettingValue()
	if err != nil {
		return false, err
	}
	update := armpostgresqlflexibleservers.Configuration{
		Properties: &armpostgresqlflexibleservers.ConfigurationProperties{
			Source: to.Ptr("user-override"),
			Value:  &value,
		},
	}

	logger.Debugf("Applying Config Parameter: %s, Value: %v\n", config.Name, config.Setting)
	updateResp, err := paramsClient.BeginPut(ctx, resourceGroupName, serverName, config.Name, update, nil)
	if err != nil {
		return false, err
	}

	for !(updateResp.Done()) {
		// is the network is flakey, Poll will just sit for ages, this timeout
		// stops that from happening. Since it is a polling loop it will retry
		// anyway. Obviously it could be bad if it retries indefinitely
		timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		resp, err := updateResp.Poll(timeoutCtx)
		if err != nil {
			logger.Warnf("Error waiting for %s: %v", config.Name, err)
		} else {
			logger.Infof("ConfigPoller status for %s: %s", config.Name, resp.Status)
		}
	}

	res, err := updateResp.Result(ctx)
	if err != nil {
		logger.Errorf("Error applying %s, %v", config.Name, err)
	}
	logger.Infof("Need to restart? %t", *res.Configuration.Properties.IsConfigPendingRestart)

	if err != nil {
		return false, err
	}
	// end of the apply cycle
	return *res.Configuration.Properties.IsConfigPendingRestart, nil
}

func (adapter *AzureFlexAdapter) GetActiveConfig(ctx context.Context) (tuning.ConfigArraySchema, error) {
	config, err := tuning.GetActiveConfig(adapter.PGDriver, ctx)
	if err != nil {
		return nil, err
	}

	return config, err
}
