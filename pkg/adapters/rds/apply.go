package rds

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/dbtuneai/agent/pkg/catalog"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/tuning"
	"github.com/sirupsen/logrus"
)

// Most circumstantial happens in RDSAdapter.ApplyConfig.
// This function simply applies the config, and reboots if necessary.
// The reboot is postponed until the parameter group change is staged,
// so the reboot actually picks it up.
func ApplyConfig(
	targetConfig []configInfo,
	clients *AWSClients,
	parameterGroupName string,
	databaseIdentifier string,
	reqRestart bool,
	logger *logrus.Logger,
	ctx context.Context,
) error {
	applyMethod := rdsTypes.ApplyMethodImmediate
	if reqRestart {
		applyMethod = rdsTypes.ApplyMethodPendingReboot
	}

	if len(targetConfig) > 0 {
		args := &rds.ModifyDBParameterGroupInput{
			DBParameterGroupName: aws.String(parameterGroupName),
			Parameters:           awsParameters(targetConfig, applyMethod),
		}
		_, err := clients.RDSClient.ModifyDBParameterGroup(ctx, args)
		if err != nil {
			return fmt.Errorf("failed to modify parameter group: %w", err)
		}
	}

	if reqRestart {
		// The write is staged asynchronously so we wait before triggering the restart.
		if err := waitParameterStaged(clients, databaseIdentifier, parameterGroupName, logger, ctx); err != nil {
			return fmt.Errorf("parameter change not staged for reboot: %w", err)
		}

		args := &rds.RebootDBInstanceInput{DBInstanceIdentifier: aws.String(databaseIdentifier)}
		if _, err := clients.RDSClient.RebootDBInstance(ctx, args); err != nil {
			return fmt.Errorf("failed to reboot RDS instance: %w", err)
		}
	}

	return nil
}

// waitParameterStaged waits for RDS to report the parameter group change pending a
// reboot, so the reboot below actually picks it up. Sleeping before the first read
// helps with the edge case where the status is pending-reboot since previously.
func waitParameterStaged(
	clients *AWSClients,
	databaseIdentifier string,
	parameterGroupName string,
	logger *logrus.Logger,
	ctx context.Context,
) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	status := ""
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"timed out waiting for parameter group %q to reach pending-reboot (last status %q)",
				parameterGroupName, status)
		case <-time.After(10 * time.Second):
		}

		instance, err := fetchRDSDBInstance(databaseIdentifier, clients, ctx)
		if err != nil {
			logger.Warnf("Could not read the parameter apply status: %v", err)
			continue
		}
		if len(instance.DBParameterGroups) > 0 {
			status = aws.ToString(instance.DBParameterGroups[0].ParameterApplyStatus)
		}
		if status == "pending-reboot" {
			return nil
		}
		logger.Infof("Waiting for RDS to stage the parameter change (status: %q)", status)
	}
}

func getRDSParameterInfo(
	clients *AWSClients,
	parameterGroupName string,
	names []string,
	ctx context.Context,
) ([]rdsTypes.Parameter, error) {
	input := &rds.DescribeDBParametersInput{
		DBParameterGroupName: aws.String(parameterGroupName),
		MaxRecords:           aws.Int32(100),
		Filters: []rdsTypes.Filter{{
			Name:   aws.String("parameter-name"),
			Values: names,
		}},
	}

	out, err := clients.RDSClient.DescribeDBParameters(ctx, input)
	if err != nil {
		return nil, err
	}
	return out.Parameters, nil
}

// Replacing the SDK's DBInstanceAvailable waiter, which succeeds only on a
// literal "available" and so times out on a healthy instance that is merely
// backing up or optimizing storage.
func waitInstanceServing(
	clients *AWSClients,
	databaseIdentifier string,
	logger *logrus.Logger,
	ctx context.Context,
) error {
	ctx, cancel := context.WithTimeout(ctx, instanceWaitTimeout)
	defer cancel()

	status := "unknown"
	for {
		instance, err := fetchRDSDBInstance(databaseIdentifier, clients, ctx)
		if err != nil {
			logger.Warnf("Could not read the instance status: %v", err)
		} else {
			status = aws.ToString(instance.DBInstanceStatus)
			switch classifyInstanceStatus(status) {
			case instanceStatusServing:
				logger.Infof("RDS reports instance %q serving (status: %q)", databaseIdentifier, status)
				return nil
			case instanceStatusTerminal:
				return fmt.Errorf(
					"instance %q is in state %q and will not come back on its own",
					databaseIdentifier, status)
			case instanceStatusBusy:
				logger.Infof("Waiting for the instance to come back (status: %q)", status)
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"gave up after %s waiting for instance %q to come back (last status %q): %w",
				instanceWaitTimeout, databaseIdentifier, status, ctx.Err())
		case <-time.After(instanceWaitInterval):
		}
	}
}

func (adapter *RDSAdapter) GetActiveConfig(ctx context.Context) (tuning.ConfigArraySchema, error) {
	return tuning.GetActiveConfig(adapter.PGDriver, ctx)
}

func (adapter *RDSAdapter) ApplyConfig(ctx context.Context, proposedConfig *tuning.ProposedConfigResponse) tuning.ApplyConfigError {
	if adapter.State.CheckApplyDebounced(1 * time.Minute) {
		adapter.Logger().Infof("Config was applied less than %s ago, skipping", 1*time.Minute)
		return nil
	}

	// Stamped on return.
	defer func() { adapter.State.LastApplyAttempt = time.Now() }()

	if err := adapter.refreshDBInfo(ctx); err != nil {
		return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to refresh DB info before apply: %w", err)}
	}
	// Check that we are not connected to the default parameter group.
	if err := defaultParameterGroupError(adapter.State.DBInfo); err != nil {
		return err
	}

	adapter.Logger().Infof(
		"Parameter group %q apply status before write: %s",
		adapter.State.DBInfo.ParameterGroupName,
		adapter.State.DBInfo.ParameterGroupStatus,
	)

	// aggregate the data from proposedConfig and the current ParameterGroup
	configs, err := getConfigInfo(
		proposedConfig, &adapter.AWSClients, adapter.State.DBInfo.ParameterGroupName, ctx)
	if err != nil {
		return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to resolve knobs to apply: %w", err)}
	}

	containsRestartParameterChange := slices.ContainsFunc(configs, configInfo.isChangedRestartParameter)
	applyMethodIsRestart := proposedConfig.KnobApplication == tuning.KnobApplicationRestart
	if applyMethodIsRestart && !config.IsRestartAllowed() {
		return &tuning.RestartNotAllowedError{
			Message: "restart is not allowed in the agent",
		}
	}
	if containsRestartParameterChange && !applyMethodIsRestart {
		return &tuning.ConfigApplyError{
			Err: errors.New("a parameter requires a restart to take effect, but the apply method is reload"),
		}
	}
	changedConfigs := []configInfo{}
	for _, c := range configs {
		if c.changed() {
			changedConfigs = append(changedConfigs, c)
		}
	}

	needsRestartFromBefore := applyMethodIsRestart &&
		adapter.State.DBInfo.ParameterGroupStatus == "pending-reboot"

	if len(changedConfigs) == 0 && !needsRestartFromBefore {
		adapter.Logger().Info("Parameter group already holds the requested values. Exiting apply.")
		return nil
	}
	adapter.Logger().Infof("Applying %d changed parameters to parameter group %q", len(changedConfigs), adapter.State.DBInfo.ParameterGroupName)

	err = ApplyConfig(
		changedConfigs,
		&adapter.AWSClients,
		adapter.State.DBInfo.ParameterGroupName,
		adapter.Config.RDSDatabaseIdentifier,
		containsRestartParameterChange || needsRestartFromBefore,
		adapter.Logger(),
		ctx,
	)
	if err != nil {
		return asApplyConfigError(err)
	}

	// RDS reports the instance available well before PostgreSQL accepts
	// connections, so both waits are needed, in this order.
	if err := waitInstanceServing(
		&adapter.AWSClients, adapter.Config.RDSDatabaseIdentifier, adapter.Logger(), ctx,
	); err != nil {
		return &tuning.ConfigApplyError{Err: fmt.Errorf("error waiting for instance to come back online: %w", err)}
	}

	adapter.Logger().Info("Waiting for PostgreSQL to come back online...")
	err = tuning.WaitPostgresReady(adapter.PGDriver, ctx)
	if err != nil {
		return &tuning.ConfigApplyError{Err: fmt.Errorf("error waiting for PostgreSQL to come back online: %w", err)}
	}

	// Check that the values get applied in PostgreSQL.
	if applyErr := adapter.verifyAppliedSettings(ctx, configs); applyErr != nil {
		return applyErr
	}

	return nil
}

// verifyAppliedSettings polls pg_settings until the server reports every value
// written, and classifies the failure otherwise.
func (adapter *RDSAdapter) verifyAppliedSettings(
	ctx context.Context,
	targets []configInfo,
) tuning.ApplyConfigError {
	if len(targets) == 0 {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, pgVerifyTimeout)
	defer cancel()

	adapter.Logger().Info("Verifying the new configuration is live in PostgreSQL...")

	names := getConfigNames(targets)
	diff := settingsDiff{Missing: names}
	for {
		rows, queryErr := catalog.QueryPgSettings(adapter.PGDriver, waitCtx)
		if queryErr != nil {
			adapter.Logger().Warnf("Could not read pg_settings while verifying the apply: %v", queryErr)
		} else {
			diff = diffPGSettings(targets, rows)
			if len(diff.Missing) > 0 {
				return &tuning.ConfigApplyError{Err: fmt.Errorf(
					"cannot verify apply: %s unknown to this PostgreSQL server",
					strings.Join(diff.Missing, ", "),
				)}
			}
			if len(diff.Mismatched) == 0 {
				adapter.Logger().Infof("Configuration verified live for %s", strings.Join(names, ", "))
				return nil
			}
			adapter.Logger().Infof("Waiting for PostgreSQL to report the new configuration: %s", diff)
		}

		select {
		case <-waitCtx.Done():
			return &tuning.ConfigApplyError{Err: fmt.Errorf(
				"timed out after %s waiting for PostgreSQL to report the new configuration (%s); %s",
				pgVerifyTimeout, diff, adapter.parameterGroupDiagnosis(ctx, targets),
			)}
		case <-time.After(pgVerifyInterval):
		}
	}
}

// parameterGroupDiagnosis says whether the group holds the requested values,
// separating a write that never stuck from one the engine never loaded.
// - Failure path only -
func (adapter *RDSAdapter) parameterGroupDiagnosis(ctx context.Context, targets []configInfo) string {
	name := adapter.State.DBInfo.ParameterGroupName

	ctx, cancel := context.WithTimeout(ctx, paramGroupReadTimeout)
	defer cancel()

	actual, err := getRDSParameterInfo(&adapter.AWSClients, name, getConfigNames(targets), ctx)
	if err != nil {
		return fmt.Sprintf("could not read parameter group %q back to narrow it down: %v", name, err)
	}
	if mismatches := groupValueMismatches(targets, actual); len(mismatches) > 0 {
		return fmt.Sprintf(
			"parameter group %q does not hold the applied config values:\n%s",
			name, strings.Join(mismatches, "\n"),
		)
	}
	return fmt.Sprintf(
		"parameter group %q does hold the requested values, but they are not live in the database.",
		name,
	)
}

// asApplyConfigError keeps a typed ApplyConfigError from anywhere in the chain,
// so the platform gets its wire type. tuning.ValidateRestartPolicy returns
// *tuning.RestartNotAllowedError, which wrapping would downgrade.
func asApplyConfigError(err error) tuning.ApplyConfigError {
	var typed tuning.ApplyConfigError
	if errors.As(err, &typed) {
		return typed
	}
	return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to apply config: %w", err)}
}
