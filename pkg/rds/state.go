package rds

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/aws/aws-sdk-go-v2/service/rds"

	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/dbtuneai/agent/pkg/internal/keywords"
	"github.com/dbtuneai/agent/pkg/internal/utils"
)

type State struct {
	LastAppliedConfig  time.Time
	LastGuardrailCheck time.Time
	LastDBInfoCheck    time.Time
	DBInfo             *DBInfo
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

	dbInfo := DBInfo{
		DBInstance:          *rdsInstanceInfo,
		EC2InstanceType:     ec2types.InstanceType(instanceType),
		EC2InstanceTypeInfo: *ec2InstanceTypeInfo,
	}
	return dbInfo, nil
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
	if err == nil {
		totalMemoryBytesMetric, err := utils.NewMetric(keywords.NodeMemoryTotal, totalMemoryBytes, utils.Int)
		if err == nil {
			metrics = append(metrics, totalMemoryBytesMetric)
		}
	}

	nVCPUs, err := info.VCPUs()
	if err == nil {
		noCPUsMetric, err := utils.NewMetric(keywords.NodeCPUCount, nVCPUs, utils.Int)
		if err == nil {
			metrics = append(metrics, noCPUsMetric)
		}
	}

	// TODO(eddie): Really? We should definitely find where to fetch this and add to system info.
	// For RDS, disk type is always SSD
	diskTypeMetric, err := utils.NewMetric(keywords.NodeStorageType, "SSD", utils.String)
	if err == nil {
		metrics = append(metrics, diskTypeMetric)
	}

	return metrics, nil
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
