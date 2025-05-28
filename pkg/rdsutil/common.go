package rdsutil

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
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/pi"
	pitypes "github.com/aws/aws-sdk-go-v2/service/pi/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/sirupsen/logrus"
)

type AWSClients struct {
	EC2Client        *ec2.Client
	RDSClient        *rds.Client
	PIClient         *pi.Client
	CloudwatchClient *cloudwatch.Client
}

func NewAWSClients(cfg aws.Config) AWSClients {
	ec2Client := ec2.NewFromConfig(cfg)
	rdsClient := rds.NewFromConfig(cfg)
	piClient := pi.NewFromConfig(cfg)
	cloudwatchClient := cloudwatch.NewFromConfig(cfg)
	return AWSClients{
		EC2Client:        ec2Client,
		RDSClient:        rdsClient,
		PIClient:         piClient,
		CloudwatchClient: cloudwatchClient,
	}
}

type DBInfo struct {
	DBInstance          rdsTypes.DBInstance
	EC2InstanceType     ec2types.InstanceType
	EC2InstanceTypeInfo ec2types.InstanceTypeInfo
}

func FetchDBInfo(
	databaseIdentifier string,
	clients *AWSClients,
	ctx context.Context,
) (DBInfo, error) {
	rdsInstanceInfo, err := fetchRDSDBInstance(databaseIdentifier, clients, ctx)
	if err != nil {
		return DBInfo{}, fmt.Errorf("failed to fetch RDS instance info: %v", err)
	}

	instanceClass := rdsInstanceInfo.DBInstanceClass
	instanceType := strings.TrimPrefix(*instanceClass, "db.")
	ec2InstanceTypeInfo, err := fetchEC2InstanceTypeInfo(ec2types.InstanceType(instanceType), clients, ctx)
	if err != nil {
		return DBInfo{}, fmt.Errorf("failed to fetch EC2 instance info: %v", err)
	}

	return DBInfo{
		DBInstance:          *rdsInstanceInfo,
		EC2InstanceType:     ec2types.InstanceType(instanceType),
		EC2InstanceTypeInfo: *ec2InstanceTypeInfo,
	}, nil
}

func (info *DBInfo) VCPUs() (uint32, error) {
	vcpuInfo := info.EC2InstanceTypeInfo.VCpuInfo
	if vcpuInfo == nil {
		return 0, fmt.Errorf("VCPU information not available")
	}

	vcpuCount := vcpuInfo.DefaultVCpus
	if vcpuCount == nil {
		return 0, fmt.Errorf("VCPU information not available")
	} else if *vcpuCount < 1 {
		return 0, fmt.Errorf("VCPU count is less than 1, got %d", *vcpuCount)
	}
	return uint32(*vcpuCount), nil
}

func (info *DBInfo) TotalMemoryBytes() (uint64, error) {
	memoryInfo := info.EC2InstanceTypeInfo.MemoryInfo
	if memoryInfo == nil {
		return 0, fmt.Errorf("memory information not available")
	}

	memorySize := memoryInfo.SizeInMiB
	if memorySize == nil {
		return 0, fmt.Errorf("memory information not available")
	} else if *memorySize < 1 {
		return 0, fmt.Errorf("memory size is less than 1, got %d", *memorySize)
	}
	return uint64(*memorySize) * 1024 * 1024, nil
}

func (info *DBInfo) PerformanceInsightsEnabled() bool {
	return info.DBInstance.PerformanceInsightsEnabled != nil && *info.DBInstance.PerformanceInsightsEnabled
}

func (info *DBInfo) ResourceID() (string, error) {
	if info.DBInstance.DbiResourceId == nil {
		return "", fmt.Errorf("DBI resource ID (ARN) not available")
	}
	return *info.DBInstance.DbiResourceId, nil
}

func (info *DBInfo) ParameterGroupStatus(name string) *rdsTypes.DBParameterGroupStatus {
	for _, pg := range info.DBInstance.DBParameterGroups {
		if aws.ToString(pg.DBParameterGroupName) == name {
			return &pg
		}
	}
	return nil
}

func (info *DBInfo) TryIntoFlatValuesSlice() ([]utils.FlatValue, error) {
	metrics := []utils.FlatValue{}

	totalMemoryBytes, err := info.TotalMemoryBytes()
	if err != nil {
		totalMemoryBytesMetric, err := utils.NewMetric("node_memory_total", totalMemoryBytes, utils.Int)
		if err == nil {
			metrics = append(metrics, totalMemoryBytesMetric)
		}
	}

	nVCPUs, err := info.VCPUs()
	if err == nil {
		noCPUsMetric, err := utils.NewMetric("node_cpu_count", nVCPUs, utils.Int)
		if err == nil {
			metrics = append(metrics, noCPUsMetric)
		}
	}

	// TODO(eddie): Really? We should definitely find where to fetch this and add to system info.
	// For RDS, disk type is always SSD
	diskTypeMetric, err := utils.NewMetric("node_disk_device_type", "SSD", utils.String)
	if err == nil {
		metrics = append(metrics, diskTypeMetric)
	}

	return metrics, nil
}

func ApplyConfig(
	proposedConfig *agent.ProposedConfigResponse,
	rdsInfo *DBInfo,
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
		return fmt.Errorf("failed to get modified parameters: %v", err)
	}

	// Modify parameter group
	args := &rds.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String(parameterGroupName),
		Parameters:           modifiedParameters,
	}

	// TODO(eddie): We should actuall verify in the response that it worked
	_, err = clients.RDSClient.ModifyDBParameterGroup(ctx, args)
	if err != nil {
		return fmt.Errorf("failed to modify parameter group: %v", err)
	}

	// Wait for parameter group changes to be processed
	logger.Info("Waiting for parameter group changes to be processed...")
	err = waitRDSInstanceAvailable(clients, databaseIdentifier, parameterGroupName, ctx)
	if err != nil {
		return fmt.Errorf("error waiting for parameter group changes to be processed: %v", err)
	}

	// If restart is required and specified
	if applyMethod == rdsTypes.ApplyMethodPendingReboot {
		args := &rds.RebootDBInstanceInput{DBInstanceIdentifier: aws.String(databaseIdentifier)}
		_, err = clients.RDSClient.RebootDBInstance(ctx, args)
		if err != nil {
			return fmt.Errorf("failed to reboot RDS instance: %v", err)
		}
	}

	// Wait for the instance to become available and PostgreSQL to be online
	waiter := rds.NewDBInstanceAvailableWaiter(clients.RDSClient)
	dbWaiterArgs := &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(databaseIdentifier)}
	err = waiter.Wait(ctx, dbWaiterArgs, 15*time.Minute)
	if err != nil {
		return fmt.Errorf("error waiting for instance: %v", err)
	}

	return nil
}

func fetchRDSDBInstance(
	rdsDatabaseIdentifier string,
	clients *AWSClients,
	ctx context.Context,
) (*rdsTypes.DBInstance, error) {
	args := &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(rdsDatabaseIdentifier)}
	describeDBInstances, err := clients.RDSClient.DescribeDBInstances(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("failed to describe database instance: %v", err)
	}

	if len(describeDBInstances.DBInstances) == 0 {
		return nil, fmt.Errorf("instance not found")
	}
	if len(describeDBInstances.DBInstances) > 1 {
		return nil, fmt.Errorf("multiple instances found for identifier %s, this is likely a configuration issue", rdsDatabaseIdentifier)
	}
	dbInstance := describeDBInstances.DBInstances[0]
	return &dbInstance, nil
}

func fetchEC2InstanceTypeInfo(
	instanceType ec2types.InstanceType,
	clients *AWSClients,
	ctx context.Context,
) (*ec2types.InstanceTypeInfo, error) {
	args := &ec2.DescribeInstanceTypesInput{InstanceTypes: []ec2types.InstanceType{instanceType}}
	describeInstanceTypes, err := clients.EC2Client.DescribeInstanceTypes(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("failed to describe instance type: %v", err)
	}

	if len(describeInstanceTypes.InstanceTypes) == 0 {
		return nil, fmt.Errorf("instance type information not found")
	}
	ec2InstanceTypeInfo := describeInstanceTypes.InstanceTypes[0]
	return &ec2InstanceTypeInfo, nil
}

func parameterGroupStatus(
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

func waitRDSInstanceAvailable(
	clients *AWSClients,
	databaseIdentifier string,
	parameterGroupName string,
	ctx context.Context,
) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	// Wait for the parameter apply status to be either pending-reboot or in-sync
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for parameter group changes to be processed")
		case <-time.After(5 * time.Second):
			rdsInstanceInfo, err := fetchRDSDBInstance(databaseIdentifier, clients, ctx)
			if err != nil {
				continue // Retry
			}

			parameterGroupStatus := parameterGroupStatus(rdsInstanceInfo, parameterGroupName)
			if parameterGroupStatus == nil {
				continue
			}

			currentParamStatus := parameterGroupStatus.ParameterApplyStatus
			if currentParamStatus == nil {
				return fmt.Errorf("parameter group '%s' not found attached to instance '%s'", parameterGroupName, databaseIdentifier)
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
			// Successes
			case "in-sync":
				return nil
			case "pending-reboot":
				return nil
			case "failed-to-apply":
				return fmt.Errorf("parameter group is in an invalid state")
			case "pending-database-upgrade":
				return fmt.Errorf("parameter group change will be applied after the DB instance is upgraded")
			default:
				return fmt.Errorf("unknown parameter apply status: %s", *currentParamStatus)
			}
		}
	}
}

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

	// Calculate memory usage percentage from freeable memory
	cpuUtilization := *latestDatapoint.Average

	return cpuUtilization, nil
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
