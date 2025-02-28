package collectors

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/pi"
	pitypes "github.com/aws/aws-sdk-go-v2/service/pi/types"
	adapters "github.com/dbtuneai/agent/pkg/adeptersinterfaces"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
)

// AuroraHardwareInfo collects hardware metrics from Aurora RDS instance using the Performance Insights API or the CloudWatch API
func AuroraHardwareInfo(auroraAdapter adapters.AuroraRDSAdapter) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {

		// PI -> granular memory used -> node_memory_used
		if auroraAdapter.GetAuroraState().PerformanceInsights != nil && auroraAdapter.GetAuroraState().PerformanceInsights.Enabled {
			memoryUsage, err := GetMemoryUsageFromPI(auroraAdapter)
			if err != nil {
				auroraAdapter.Logger().Errorf("failed to get memory usage from Performance Insights: %v", err)
			} else {
				memoryMetric, err := utils.NewMetric("node_memory_used", memoryUsage, utils.Bytes)
				if err != nil {
					auroraAdapter.Logger().Errorf("failed to create memory metric: %v", err)
				}
				state.AddMetric(memoryMetric)
			}
		}

		// CloudWatch -> freeable memory -> node_memory_freeable
		freeableMemory, err := GetFreeableMemoryFromCW(auroraAdapter)
		if err != nil {
			auroraAdapter.Logger().Errorf("failed to get freeable memory from CloudWatch: %v", err)
		} else {
			freeableMemoryMetric, _ := utils.NewMetric("node_memory_freeable", freeableMemory, utils.Bytes)
			state.AddMetric(freeableMemoryMetric)
		}

		// CPU Utilization
		cpuUtil, err := getCPUUtilization(auroraAdapter)
		if err != nil {
			auroraAdapter.Logger().Errorf("failed to get CPU utilization: %v", err)
		} else {
			cpuUtilMetric, _ := utils.NewMetric("node_cpu_usage", cpuUtil, utils.Percentage)
			state.AddMetric(cpuUtilMetric)
		}

		return nil
	}
}

// DatapointConstraint is a type constraint that allows either types.Datapoint or pi.DataPoint
type DatapointConstraint interface {
	types.Datapoint | pitypes.DataPoint
}

// getLastDatapoint retrieves the latest datapoint from a list of
// types.Datapoint or pitypes.DataPoint
func getLastDatapoint[T DatapointConstraint](datapoints []T) (*T, error) {
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

func getCPUUtilization(aurora adapters.AuroraRDSAdapter) (float64, error) {
	endTime := time.Now()
	startTime := endTime.Add(-5 * time.Minute)

	input := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/RDS"),
		MetricName: aws.String("CPUUtilization"),
		StartTime:  aws.Time(startTime),
		EndTime:    aws.Time(endTime),
		Period:     aws.Int32(300),
		Statistics: []types.Statistic{types.Statistic("Average")},
		Dimensions: []types.Dimension{
			{
				Name:  aws.String("DBInstanceIdentifier"),
				Value: aws.String(aurora.GetAuroraRDSConfig().RDSDatabaseIdentifier),
			},
		},
	}

	output, err := aurora.GetCWClient().GetMetricStatistics(context.Background(), input)
	if err != nil {
		return 0, fmt.Errorf("failed to get CloudWatch metrics: %v", err)
	}

	latestDatapoint, err := getLastDatapoint(output.Datapoints)
	if err != nil {
		return 0, err
	}

	// Calculate memory usage percentage from freeable memory
	cpuUtilization := *latestDatapoint.Average

	return cpuUtilization, nil
}

// GetMemoryUsageFromPI retrieves memory usage (bytes) using Performance Insights
func GetMemoryUsageFromPI(aurora adapters.AuroraRDSAdapter) (uint64, error) {
	if aurora.GetAuroraState().Hardware == nil {
		return 0, fmt.Errorf("hardware information not available, skipping memory usage check")
	}

	if aurora.GetAuroraState().PerformanceInsights == nil || aurora.GetAuroraState().PerformanceInsights.ResourceID == "" {
		return 0, fmt.Errorf("performance insights ARN not available")
	}

	endTime := time.Now()
	startTime := endTime.Add(-5 * time.Minute)

	// Get the Performance Insights metrics for memory
	input := &pi.GetResourceMetricsInput{
		Identifier:      aws.String(aurora.GetAuroraState().PerformanceInsights.ResourceID),
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

	output, err := aurora.GetPIClient().GetResourceMetrics(context.Background(), input)
	if err != nil {
		aurora.Logger().Warnf("PI Input: %+v", input)
		return 0, fmt.Errorf("failed to get PI metrics: %v", err)
	}

	if len(output.MetricList) == 0 {
		return 0, fmt.Errorf("no metrics returned from Performance Insights")
	}

	if len(output.MetricList[0].DataPoints) == 0 {
		return 0, fmt.Errorf("no datapoints returned from Performance Insights")
	}

	aurora.Logger().Infof("Got %d metrics with %d datapoints", len(output.MetricList), len(output.MetricList[0].DataPoints))

	latestDatapoint, err := getLastDatapoint(output.MetricList[0].DataPoints)
	if err != nil {
		return 0, err
	}

	activeMemoryBytes := *latestDatapoint.Value * 1024 // It is returned in KB

	if activeMemoryBytes < 0 {
		return 0, fmt.Errorf("active memory is negative")
	}

	return uint64(activeMemoryBytes), nil
}

// GetFreeableMemoryFromCW retrieves freeable memory (bytes) using CloudWatch
func GetFreeableMemoryFromCW(aurora adapters.AuroraRDSAdapter) (uint64, error) {
	if aurora.GetAuroraState().Hardware == nil {
		return 0, fmt.Errorf("hardware information not available, skipping memory usage check")
	}

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
				Value: aws.String(aurora.GetAuroraRDSConfig().RDSDatabaseIdentifier),
			},
		},
	}

	output, err := aurora.GetCWClient().GetMetricStatistics(context.Background(), input)
	if err != nil {
		return 0, fmt.Errorf("failed to get CloudWatch metrics: %v", err)
	}

	if len(output.Datapoints) == 0 {
		return 0, fmt.Errorf("no metrics available")
	}

	// Get the most recent datapoint
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
