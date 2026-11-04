package rds

import (
	"context"
	"fmt"
	"strings"
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
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
		return 0, fmt.Errorf("failed to get PI metrics: %w", err)
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
		return 0, fmt.Errorf("failed to get CloudWatch metrics: %w", err)
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

// AWS PRM attribution token. Appended via raw header middleware below, not AddUserAgentKey(Value) -
// those sanitize "/" to "-" and corrupt this format (confirmed live via CloudTrail).
const awsPartnerAttributionUserAgent = "APN_1.1/pc_dml4zrdqnmoacdcn4u6vmz9q4$"

// Must run after the SDK's "UserAgent" middleware, or that middleware overwrites this.
type prmUserAgentMiddleware struct{}

func (prmUserAgentMiddleware) ID() string { return "DBtunePRMUserAgent" }

func (prmUserAgentMiddleware) HandleBuild(
	ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
) (middleware.BuildOutput, middleware.Metadata, error) {
	if req, ok := in.Request.(*smithyhttp.Request); ok {
		req.Header.Set("User-Agent", strings.TrimLeft(req.Header.Get("User-Agent")+" "+awsPartnerAttributionUserAgent, " "))
	}
	return next.HandleBuild(ctx, in)
}

func addPRMUserAgent(stack *middleware.Stack) error {
	return stack.Build.Insert(prmUserAgentMiddleware{}, "UserAgent", middleware.After)
}

func FetchAWSConfig(
	awsAccessKey string,
	awsSecretAccessKey string,
	awsRegion string,
	ctx context.Context,
) (aws.Config, error) {
	region := config.WithRegion(awsRegion)
	apiOptions := config.WithAPIOptions([]func(*middleware.Stack) error{
		addPRMUserAgent,
	})
	if awsAccessKey != "" && awsSecretAccessKey != "" {
		// Use static credentials if provided
		creds := credentials.NewStaticCredentialsProvider(awsAccessKey, awsSecretAccessKey, "")
		provider := config.WithCredentialsProvider(creds)
		return config.LoadDefaultConfig(ctx, region, provider, apiOptions)
	} else {
		// Use default credential chain
		// Includes by default WebIdentityToken:
		// https://github.com/aws/aws-sdk-go-v2/blob/main/config/resolve_credentials.go#L119
		return config.LoadDefaultConfig(ctx, region, apiOptions)
	}
}

// Most circumstantial happens in ApplyConfig in pkg/rds/adapters.go.
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

// Timing for the instance wait. A reboot is minutes; past this something else
// is going on and the error should say what.
const (
	instanceWaitTimeout  = 15 * time.Minute
	instanceWaitInterval = 15 * time.Second
)

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
		return 0, fmt.Errorf("failed to get CloudWatch metrics: %w", err)
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
