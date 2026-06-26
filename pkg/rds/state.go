package rds

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/aws/aws-sdk-go-v2/service/rds"

	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
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
	// ServerlessMaxACUs is set for Aurora Serverless v2 instances (db.serverless).
	// 1 ACU = 2 GiB memory. Nil for non-serverless instances.
	ServerlessMaxACUs *float64
	// ParameterGroupName is the instance-level DB parameter group currently
	// attached to the instance, discovered via DescribeDBInstances. Empty if
	// the instance reports no parameter groups (should not happen for RDS/Aurora).
	ParameterGroupName string
	// ClusterParameterGroupName is the cluster-level DB parameter group for
	// Aurora and Multi-AZ DB clusters, discovered via DescribeDBClusters.
	// Empty for single-instance RDS (no DBClusterIdentifier on the instance).
	ClusterParameterGroupName string
}

func FetchDBInfo(
	databaseIdentifier string,
	clients *AWSClients,
	ctx context.Context,
) (DBInfo, error) {
	rdsInstanceInfo, err := fetchRDSDBInstance(databaseIdentifier, clients, ctx)
	if err != nil {
		return DBInfo{}, fmt.Errorf("failed to fetch RDS instance info: %w", err)
	}

	instanceClass := rdsInstanceInfo.DBInstanceClass
	instanceType := strings.TrimPrefix(*instanceClass, "db.")

	parameterGroupName := ""
	if len(rdsInstanceInfo.DBParameterGroups) > 0 {
		parameterGroupName = aws.ToString(rdsInstanceInfo.DBParameterGroups[0].DBParameterGroupName)
	}

	var cluster *auroraClusterInfo
	if rdsInstanceInfo.DBClusterIdentifier != nil {
		clusterID := aws.ToString(rdsInstanceInfo.DBClusterIdentifier)
		cluster, err = fetchAuroraClusterInfo(clusterID, clients, ctx)
		if err != nil {
			return DBInfo{}, fmt.Errorf("failed to fetch DB cluster info: %w", err)
		}
	}

	clusterParameterGroupName := ""
	if cluster != nil {
		clusterParameterGroupName = cluster.ParameterGroupName
	}

	// Aurora Serverless v2 uses "db.serverless" which is not a valid EC2 instance type.
	// Fetch capacity from the cluster instead: 1 ACU = 2 GiB memory.
	if instanceType == "serverless" {
		if cluster == nil || cluster.MaxACUs == nil {
			return DBInfo{}, fmt.Errorf("ServerlessV2ScalingConfiguration not available for cluster %s", aws.ToString(rdsInstanceInfo.DBClusterIdentifier))
		}
		return DBInfo{
			DBInstance:                *rdsInstanceInfo,
			ServerlessMaxACUs:         cluster.MaxACUs,
			ParameterGroupName:        parameterGroupName,
			ClusterParameterGroupName: clusterParameterGroupName,
		}, nil
	}

	ec2InstanceTypeInfo, err := fetchEC2InstanceTypeInfo(ec2types.InstanceType(instanceType), clients, ctx)
	if err != nil {
		return DBInfo{}, fmt.Errorf("failed to fetch EC2 instance info: %w", err)
	}

	dbInfo := DBInfo{
		DBInstance:                *rdsInstanceInfo,
		EC2InstanceType:           ec2types.InstanceType(instanceType),
		EC2InstanceTypeInfo:       *ec2InstanceTypeInfo,
		ParameterGroupName:        parameterGroupName,
		ClusterParameterGroupName: clusterParameterGroupName,
	}
	return dbInfo, nil
}

func (info *DBInfo) VCPUs() (uint32, error) {
	// Aurora Serverless v2: approximate 1 ACU ≈ 1 vCPU
	if info.ServerlessMaxACUs != nil {
		return uint32(math.Ceil(*info.ServerlessMaxACUs)), nil
	}

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
	// Aurora Serverless v2: 1 ACU = 2 GiB memory
	if info.ServerlessMaxACUs != nil {
		return uint64(*info.ServerlessMaxACUs * 2 * 1024 * 1024 * 1024), nil
	}

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

// defaultParameterGroupError returns a typed ApplyConfigError if the instance's
// attached parameter group is an AWS-managed default.* group (which cannot be
// modified). Returns nil otherwise, including when dbInfo is nil or the group
// name is empty.
func defaultParameterGroupError(dbInfo *DBInfo) *agent.DefaultParameterGroupError {
	if dbInfo == nil {
		return nil
	}
	if !strings.HasPrefix(dbInfo.ParameterGroupName, "default.") {
		return nil
	}
	return &agent.DefaultParameterGroupError{ParameterGroupName: dbInfo.ParameterGroupName}
}

func (info *DBInfo) ParameterGroupStatus(name string) *rdsTypes.DBParameterGroupStatus {
	for _, pg := range info.DBInstance.DBParameterGroups {
		if aws.ToString(pg.DBParameterGroupName) == name {
			return &pg
		}
	}
	return nil
}

func (info *DBInfo) TryIntoFlatValuesSlice() ([]metrics.FlatValue, error) {
	flat_metrics := []metrics.FlatValue{}

	totalMemoryBytes, err := info.TotalMemoryBytes()
	if err == nil {
		totalMemoryBytesMetric, err := metrics.NodeMemoryTotal.AsFlatValue(totalMemoryBytes)
		if err == nil {
			flat_metrics = append(flat_metrics, totalMemoryBytesMetric)
		}
	}

	nVCPUs, err := info.VCPUs()
	if err == nil {
		noCPUsMetric, err := metrics.NodeCPUCount.AsFlatValue(nVCPUs)
		if err == nil {
			flat_metrics = append(flat_metrics, noCPUsMetric)
		}
	}

	if info.ParameterGroupName != "" {
		if v, err := metrics.AWSRDSParameterGroup.AsFlatValue(info.ParameterGroupName); err == nil {
			flat_metrics = append(flat_metrics, v)
		}
	}
	if info.ClusterParameterGroupName != "" {
		if v, err := metrics.AWSRDSClusterParameterGroup.AsFlatValue(info.ClusterParameterGroupName); err == nil {
			flat_metrics = append(flat_metrics, v)
		}
	}

	return flat_metrics, nil
}

func fetchRDSDBInstance(
	rdsDatabaseIdentifier string,
	clients *AWSClients,
	ctx context.Context,
) (*rdsTypes.DBInstance, error) {
	args := &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(rdsDatabaseIdentifier)}
	describeDBInstances, err := clients.RDSClient.DescribeDBInstances(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("failed to describe database instance: %w", err)
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

// auroraClusterInfo holds the fields read from DescribeDBClusters that the
// agent cares about. MaxACUs is nil for non-Serverless-v2 clusters.
type auroraClusterInfo struct {
	MaxACUs            *float64
	ParameterGroupName string
}

func fetchAuroraClusterInfo(
	clusterIdentifier string,
	clients *AWSClients,
	ctx context.Context,
) (*auroraClusterInfo, error) {
	input := &rds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterIdentifier),
	}
	result, err := clients.RDSClient.DescribeDBClusters(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to describe DB cluster: %w", err)
	}
	if len(result.DBClusters) == 0 {
		return nil, fmt.Errorf("cluster not found: %s", clusterIdentifier)
	}
	cluster := result.DBClusters[0]
	info := &auroraClusterInfo{
		ParameterGroupName: aws.ToString(cluster.DBClusterParameterGroup),
	}
	if scaling := cluster.ServerlessV2ScalingConfiguration; scaling != nil {
		info.MaxACUs = scaling.MaxCapacity
	}
	return info, nil
}

func fetchEC2InstanceTypeInfo(
	instanceType ec2types.InstanceType,
	clients *AWSClients,
	ctx context.Context,
) (*ec2types.InstanceTypeInfo, error) {
	args := &ec2.DescribeInstanceTypesInput{InstanceTypes: []ec2types.InstanceType{instanceType}}
	describeInstanceTypes, err := clients.EC2Client.DescribeInstanceTypes(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("failed to describe instance type: %w", err)
	}

	if len(describeInstanceTypes.InstanceTypes) == 0 {
		return nil, fmt.Errorf("instance type information not found")
	}
	ec2InstanceTypeInfo := describeInstanceTypes.InstanceTypes[0]
	return &ec2InstanceTypeInfo, nil
}
