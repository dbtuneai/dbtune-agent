package rds

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/pi"
	pitypes "github.com/aws/aws-sdk-go-v2/service/pi/types"
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

func (prmUserAgentMiddleware) ID() string { return "DBtunePRMUserAgent" }

func (prmUserAgentMiddleware) HandleBuild(
	ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
) (middleware.BuildOutput, middleware.Metadata, error) {
	if req, ok := in.Request.(*smithyhttp.Request); ok {
		req.Header.Set("User-Agent", strings.TrimLeft(req.Header.Get("User-Agent")+" "+awsPartnerAttributionUserAgent, " "))
	}
	return next.HandleBuild(ctx, in)
}

// Timing for the instance wait. A reboot is minutes; past this something else
// is going on and the error should say what.
const (
	instanceWaitTimeout  = 15 * time.Minute
	instanceWaitInterval = 15 * time.Second
)

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
