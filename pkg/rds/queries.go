package rds

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/pi"
	pitypes "github.com/aws/aws-sdk-go-v2/service/pi/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/sirupsen/logrus"
)

// GetMemoryUsageFromPI retrieves memory usage (bytes) using Performance Insights
func GetMemoryUsageFromPI(
	clients *AWSClients,
	resourceID string,
	logger *logrus.Logger,
) (uint64, error) {
	endTime := time.Now()
	startTime := endTime.Add(-5 * time.Minute)

	// Get the Performance Insights metrics for memory
	input := &pi.GetResourceMetricsInput{
		Identifier:      aws.String(resourceID),
		ServiceType:     pitypes.ServiceTypeRds,
		StartTime:       aws.Time(startTime),
		EndTime:         aws.Time(endTime),
		PeriodInSeconds: aws.Int32(300),
		MetricQueries: []pitypes.MetricQuery{
			{
				Metric: aws.String("os.memory.active.avg"), // Append the aggregate function
			},
		},
	}

	output, err := clients.PIClient.GetResourceMetrics(context.Background(), input)
	if err != nil {
		logger.Warnf("PI Input: %+v", input)
		return 0, fmt.Errorf("failed to get PI metrics: %v", err)
	}

	if len(output.MetricList) == 0 {
		return 0, fmt.Errorf("no metrics returned from Performance Insights")
	}

	if len(output.MetricList[0].DataPoints) == 0 {
		return 0, fmt.Errorf("no datapoints returned from Performance Insights")
	}

	logger.Infof("Got %d metrics with %d datapoints", len(output.MetricList), len(output.MetricList[0].DataPoints))

	latestDatapoint, err := getLastDatapoint(output.MetricList[0].DataPoints)
	if err != nil {
		return 0, err
	}

	if latestDatapoint.Value == nil {
		return 0, fmt.Errorf("no value returned from Performance Insights")
	}

	activeMemoryBytes := *latestDatapoint.Value * 1024 // It is returned in KB

	if activeMemoryBytes < 0 {
		return 0, fmt.Errorf("active memory is negative")
	}

	return uint64(activeMemoryBytes), nil
}

// GetFreeableMemoryFromCW retrieves freeable memory (bytes) using CloudWatch
func GetFreeableMemoryFromCW(
	databaseIdentifier string,
	clients *AWSClients,
) (uint64, error) {
	endTime := time.Now()
	startTime := endTime.Add(-5 * time.Minute)

	input := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/RDS"),
		MetricName: aws.String("FreeableMemory"),
		StartTime:  aws.Time(startTime),
		EndTime:    aws.Time(endTime),
		Period:     aws.Int32(300),
		Statistics: []types.Statistic{types.Statistic("Average")},
		Dimensions: []types.Dimension{
			{
				Name:  aws.String("DBInstanceIdentifier"),
				Value: aws.String(databaseIdentifier),
			},
		},
	}

	output, err := clients.CloudwatchClient.GetMetricStatistics(context.Background(), input)
	if err != nil {
		return 0, fmt.Errorf("failed to get CloudWatch metrics: %v", err)
	}

	if len(output.Datapoints) == 0 {
		return 0, fmt.Errorf("no metrics available")
	}

	// Get the most recent datapoint
	// TODO(eddie): Is this really unordered?
	var latestDatapoint *types.Datapoint
	for _, dp := range output.Datapoints {
		if latestDatapoint == nil || dp.Timestamp.After(*latestDatapoint.Timestamp) {
			latestDatapoint = &dp
		}
	}

	if latestDatapoint == nil {
		return 0, fmt.Errorf("no datapoints available")
	}

	// Calculate memory usage percentage from freeable memory
	freeableMemoryBytes := *latestDatapoint.Average

	if freeableMemoryBytes < 0 {
		return 0, fmt.Errorf("freeable memory is negative")
	}

	return uint64(freeableMemoryBytes), nil
}

func GetCPUUtilization(
	databaseIdentifier string,
	clients *AWSClients,
) (float64, error) {
	return getAverageMetricValue(clients, databaseIdentifier, "CPUUtilization", 5)
}

func GetReadIOPS(
	databaseIdentifier string,
	clients *AWSClients,
) (float64, error) {
	return getAverageMetricValue(clients, databaseIdentifier, "ReadIOPS", 5)
}

func GetWriteIOPS(
	databaseIdentifier string,
	clients *AWSClients,
) (float64, error) {
	return getAverageMetricValue(clients, databaseIdentifier, "WriteIOPS", 5)
}

type IOPSResult struct {
	ReadIOPS  float64
	WriteIOPS float64
	TotalIOPS float64
}

func GetIOPS(
	databaseIdentifier string,
	clients *AWSClients,
) (IOPSResult, error) {
	readIOPS, err := getAverageMetricValue(clients, databaseIdentifier, "ReadIOPS", 5)
	if err != nil {
		return IOPSResult{}, err
	}

	writeIOPS, err := getAverageMetricValue(clients, databaseIdentifier, "WriteIOPS", 5)
	if err != nil {
		return IOPSResult{}, err
	}

	return IOPSResult{
		ReadIOPS:  readIOPS,
		WriteIOPS: writeIOPS,
		TotalIOPS: readIOPS + writeIOPS,
	}, nil
}

// RDSDatapointConstraint is a type constraint that allows either types.Datapoint or pi.DataPoint
type RDSDatapointConstraint interface {
	types.Datapoint | pitypes.DataPoint
}

// getLastDatapoint retrieves the latest datapoint from a list of
// types.Datapoint or pitypes.DataPoint
func getLastDatapoint[T RDSDatapointConstraint](datapoints []T) (*T, error) {
	if len(datapoints) == 0 {
		return nil, fmt.Errorf("no metrics available")
	}

	// Track the index of the latest datapoint
	latestIdx := -1
	var latestTime *time.Time

	// Find the datapoint with the latest timestamp
	for i, dp := range datapoints {
		// Get the timestamp based on the actual type
		var timestamp *time.Time
		switch any(dp).(type) {
		case types.Datapoint:
			// For CloudWatch datapoints
			timestamp = any(dp).(types.Datapoint).Timestamp
		case pitypes.DataPoint:
			// For Performance Insights datapoints
			timestamp = any(dp).(pitypes.DataPoint).Timestamp
		}

		if timestamp == nil {
			continue
		}

		// Update latest if this is the first valid datapoint or if it's newer
		if latestTime == nil || timestamp.After(*latestTime) {
			latestIdx = i
			latestTime = timestamp
		}
	}

	if latestIdx == -1 {
		return nil, fmt.Errorf("no datapoint found, should not happen")
	}

	return &datapoints[latestIdx], nil
}

func FetchAWSConfig(
	AWSAccessKey string,
	AWSSecretAccessKey string,
	AWSRegion string,
	ctx context.Context,
) (aws.Config, error) {
	region := config.WithRegion(AWSRegion)
	if AWSAccessKey != "" && AWSSecretAccessKey != "" {
		// Use static credentials if provided
		creds := credentials.NewStaticCredentialsProvider(AWSAccessKey, AWSSecretAccessKey, "")
		provider := config.WithCredentialsProvider(creds)
		return config.LoadDefaultConfig(ctx, region, provider)
	} else {
		// Use default credential chain
		// Includes by default WebIdentityToken:
		// https://github.com/aws/aws-sdk-go-v2/blob/main/config/resolve_credentials.go#L119
		return config.LoadDefaultConfig(ctx, region)
	}
}

func ApplyConfig(
	proposedConfig *agent.ProposedConfigResponse,
	clients *AWSClients,
	parameterGroupName string,
	databaseIdentifier string,
	logger *logrus.Logger,
	ctx context.Context,
) error {
	logger.Infof("Applying Config: %s", proposedConfig.KnobApplication)

	// Prepare parameters for modification
	var applyMethod rdsTypes.ApplyMethod
	switch proposedConfig.KnobApplication {
	case "restart":
		applyMethod = rdsTypes.ApplyMethodPendingReboot
	case "reload":
		applyMethod = rdsTypes.ApplyMethodImmediate
	case "":
		// TODO(eddie): We should make this more explicit somehow.
		// This happens when nothing is sent from the backend about this.
		// We should send an explicit string instead of leaving it blank.
		applyMethod = rdsTypes.ApplyMethodImmediate
	default:
		return fmt.Errorf("unknown knob application: %s", proposedConfig.KnobApplication)
	}

	modifiedParameters, err := modifiedParametersToApply(proposedConfig, applyMethod)
	if err != nil {
		return fmt.Errorf("failed to get modified parameters: %w", err)
	}

	// Modify parameter group
	args := &rds.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String(parameterGroupName),
		Parameters:           modifiedParameters,
	}

	// TODO(eddie): We should actually verify in the response that it worked
	_, err = clients.RDSClient.ModifyDBParameterGroup(ctx, args)
	if err != nil {
		return fmt.Errorf("failed to modify parameter group: %w", err)
	}

	// Wait for parameter group changes to be processed
	logger.Info("Waiting for parameter group changes to be processed...")
	result, err := GetRDSInstanceWithParameterGroupStatus(clients, databaseIdentifier, parameterGroupName, ctx)
	if err != nil {
		return fmt.Errorf("error waiting for parameter group changes to be processed: %w", err)
	}

	switch result.Status {
	case ParamGroupInSync:
		// Rather than early return here, we go through the InstanceAvailableWaiter as we may not have the most
		// up to date information.
		logger.Info("Parameter group changes applied and active on database")
	case ParamGroupPending:
		if applyMethod == rdsTypes.ApplyMethodPendingReboot {
			logger.Info("Parameter group changes applied, will attempt to restart database")
		} else {
			return fmt.Errorf("parameter group changes are unexpectedly in a pending status, could not apply config")
		}
	case ParamGroupFailed:
		return fmt.Errorf("parameter group changes failed to apply with status: %s", *result.StatusMessage)
	case ParamGroupMissing:
		// NOTE: This shouldn't happen unless the parameter group suddenly changed.
		return fmt.Errorf("parameter group '%s' not found attached to instance '%s'", parameterGroupName, databaseIdentifier)
	case ParamGroupUnknown:
		return fmt.Errorf("unknown parameter group status: %s", *result.StatusMessage)
	}

	// If restart is required and specified
	if applyMethod == rdsTypes.ApplyMethodPendingReboot {
		args := &rds.RebootDBInstanceInput{DBInstanceIdentifier: aws.String(databaseIdentifier)}
		_, err = clients.RDSClient.RebootDBInstance(ctx, args)
		if err != nil {
			return fmt.Errorf("failed to reboot RDS instance: %w", err)
		}
	}

	// Wait for the instance to become available and PostgreSQL to be online
	waiter := rds.NewDBInstanceAvailableWaiter(clients.RDSClient)
	dbWaiterArgs := &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(databaseIdentifier)}
	err = waiter.Wait(ctx, dbWaiterArgs, 15*time.Minute)
	if err != nil {
		return fmt.Errorf("error waiting for instance: %w", err)
	}

	// Anything but an in-sync parameter group by this point is an error.
	// TODO(eddiebergman): If not, we are left in a weird state, if tuning,
	// then it would be safer to abort. However we have no way to trigger
	// this behaviour from this point. It would be useful for the agent to
	// return log messages to the server and let the user decide what to do.
	// If we are doing something like "apply best config", then we should
	// also send a message, but there would be no tuning to abort.
	// When someone comes to revisit this logic. consider `ApplyConfig` should
	// instead only consider failures to communicate as errors, and return the
	// status of parameger group application as a result.
	result, err = GetRDSInstanceWithParameterGroupStatus(clients, databaseIdentifier, parameterGroupName, ctx)
	if err != nil {
		return fmt.Errorf("error waiting for instance: %w", err)
	}

	if result.Status != ParamGroupInSync {
		return fmt.Errorf("instance rebooted, but parameter group changes are not in-sync, but instead: %s", *result.StatusMessage)
	}
	return nil
}

func findParameterGroup(
	rdsInstanceInfo *rdsTypes.DBInstance,
	parameterGroupName string,
) *rdsTypes.DBParameterGroupStatus {
	for _, pg := range rdsInstanceInfo.DBParameterGroups {
		if aws.ToString(pg.DBParameterGroupName) == parameterGroupName {
			return &pg
		}
	}
	return nil
}

func modifiedParametersToApply(
	proposedConfig *agent.ProposedConfigResponse,
	applyMethod rdsTypes.ApplyMethod,
) ([]rdsTypes.Parameter, error) {
	// TODO(eddie): This is N^2 as FindRecommendedKnob does it's own loop -_-
	var modifiedParameters []rdsTypes.Parameter
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return nil, fmt.Errorf("failed to find recommended knob: %v", err)
		}
		fmtValue, err := knobConfig.GetSettingValue()
		if err != nil {
			return nil, fmt.Errorf("failed to get setting value: %v", err)
		}

		param := rdsTypes.Parameter{
			ParameterName:  aws.String(knobConfig.Name),
			ParameterValue: aws.String(fmtValue),
			ApplyMethod:    applyMethod,
		}
		modifiedParameters = append(modifiedParameters, param)
	}
	return modifiedParameters, nil
}

type ParameterGroupStatus string

const (
	ParamGroupInSync  ParameterGroupStatus = "in-sync" // absorbs in-sync
	ParamGroupPending ParameterGroupStatus = "pending" // absorbs pending-reboot, pending-database-upgrade
	ParamGroupFailed  ParameterGroupStatus = "failed"  // absorbs failed-to-apply
	ParamGroupMissing ParameterGroupStatus = "missing" // When parameter group does not exist on instance
	ParamGroupUnknown ParameterGroupStatus = "unknown" // absorbs all other
)

type ParameterGroupStatusResult struct {
	DBInstance    *rdsTypes.DBInstance
	Status        ParameterGroupStatus
	StatusMessage *string
}

// Get the RDS instance information, along with the parameter group status.
//
// If the parameter group is in an "applying" state, we will wait until the database is
// available. The caller may use the ParameterGroupStatus to see if the current `Status` is
// considered an error state or not.
//
// An error explicitly indicates a timeout occured.
func GetRDSInstanceWithParameterGroupStatus(
	clients *AWSClients,
	databaseIdentifier string,
	parameterGroupName string,
	ctx context.Context,
) (ParameterGroupStatusResult, error) { // error signifies timeout
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	nullResult := ParameterGroupStatusResult{
		Status:        ParamGroupUnknown,
		StatusMessage: nil,
		DBInstance:    nil,
	}
	for {
		select {
		case <-ctx.Done():
			return nullResult, fmt.Errorf("timeout waiting for parameter group changes to be processed")
		case <-time.After(5 * time.Second):
			rdsInstanceInfo, err := FetchRDSDBInstance(databaseIdentifier, clients, ctx)
			if err != nil {
				return nullResult, fmt.Errorf("failed to fetch RDS instance information: %w", err)
			}

			parameterGroupStatus := findParameterGroup(rdsInstanceInfo, parameterGroupName)
			if parameterGroupStatus == nil {
				return ParameterGroupStatusResult{
					DBInstance:    rdsInstanceInfo,
					Status:        ParamGroupMissing,
					StatusMessage: nil,
				}, nil
			}

			// NOTE: It can also be a null string...
			currentParamStatus := parameterGroupStatus.ParameterApplyStatus
			if currentParamStatus == nil {
				return ParameterGroupStatusResult{
					DBInstance:    rdsInstanceInfo,
					Status:        ParamGroupUnknown,
					StatusMessage: nil,
				}, nil
			}

			// Pulled from their docs for the `status` string
			// - applying : The parameter group change is being applied to the database.
			// - failed-to-apply : The parameter group is in an invalid state.
			// - in-sync : The parameter group change is synchronized with the database.
			// - pending-database-upgrade : The parameter group change will be applied after the DB instance is upgraded.
			// - pending-reboot : The parameter group change will be applied after the DB instance reboots.
			switch *currentParamStatus {
			// Waiting
			case "applying":
				continue
			// Success, happy path
			case "in-sync":
				return ParameterGroupStatusResult{
					DBInstance:    rdsInstanceInfo,
					Status:        ParamGroupInSync,
					StatusMessage: currentParamStatus,
				}, nil
			case "failed-to-apply":
				return ParameterGroupStatusResult{
					DBInstance:    rdsInstanceInfo,
					Status:        ParamGroupFailed,
					StatusMessage: currentParamStatus,
				}, nil
			case "pending-database-upgrade", "pending-reboot":
				return ParameterGroupStatusResult{
					DBInstance:    rdsInstanceInfo,
					Status:        ParamGroupPending,
					StatusMessage: currentParamStatus,
				}, nil
			default:
				return ParameterGroupStatusResult{
					DBInstance:    rdsInstanceInfo,
					Status:        ParamGroupUnknown,
					StatusMessage: currentParamStatus,
				}, nil
			}
		}
	}
}

func getAverageMetricValue(
	clients *AWSClients,
	databaseIdentifier string,
	metricName string,
	minutes uint16,
) (float64, error) {
	endTime := time.Now()
	startTime := endTime.Add(-time.Duration(minutes) * time.Minute)

	input := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/RDS"),
		MetricName: aws.String(metricName),
		StartTime:  aws.Time(startTime),
		EndTime:    aws.Time(endTime),
		Period:     aws.Int32(int32(minutes * 60)), // Needs to be multiple of 60
		Statistics: []types.Statistic{types.StatisticAverage},
		Dimensions: []types.Dimension{
			{
				Name:  aws.String("DBInstanceIdentifier"),
				Value: aws.String(databaseIdentifier),
			},
		},
	}

	output, err := clients.CloudwatchClient.GetMetricStatistics(context.Background(), input)
	if err != nil {
		return 0, fmt.Errorf("failed to get CloudWatch metrics: %v", err)
	}

	latestDatapoint, err := getLastDatapoint(output.Datapoints)
	if err != nil {
		return 0, err
	}

	if latestDatapoint.Average == nil {
		return 0, fmt.Errorf("no value returned from CloudWatch for %s", metricName)
	}

	// Calculate memory usage percentage from freeable memory
	value := *latestDatapoint.Average

	return value, nil
}
