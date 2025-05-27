package rdsutil

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	collectors "github.com/dbtuneai/agent/pkg/collectors"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EC2InstanceInfo struct {
	InstanceType     ec2types.InstanceType
	InstanceTypeInfo ec2types.InstanceTypeInfo
}

func (info *EC2InstanceInfo) VCPUs() (uint32, error) {
	vcpuInfo := info.InstanceTypeInfo.VCpuInfo
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

func (info *EC2InstanceInfo) TotalMemoryBytes() (uint64, error) {
	memoryInfo := info.InstanceTypeInfo.MemoryInfo
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

func FetchEC2InstanceInfo(
	instanceType ec2types.InstanceType,
	ec2Client *ec2.Client,
	ctx context.Context,
) (*EC2InstanceInfo, error) {
	args := &ec2.DescribeInstanceTypesInput{InstanceTypes: []ec2types.InstanceType{instanceType}}
	describeInstanceTypes, err := ec2Client.DescribeInstanceTypes(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("failed to describe instance type: %v", err)
	}

	if len(describeInstanceTypes.InstanceTypes) == 0 {
		return nil, fmt.Errorf("instance type information not found")
	}
	return &EC2InstanceInfo{
		InstanceType:     instanceType,
		InstanceTypeInfo: describeInstanceTypes.InstanceTypes[0],
	}, nil
}

type RDSInstanceInfo struct {
	InstanceType rdsTypes.DBInstance
}

func (info *RDSInstanceInfo) FetchEC2InstanceInfo(
	ec2Client *ec2.Client,
	ctx context.Context,
) (*EC2InstanceInfo, error) {
	instanceClass := info.InstanceType.DBInstanceClass
	instanceType := strings.TrimPrefix(*instanceClass, "db.")
	return FetchEC2InstanceInfo(ec2types.InstanceType(instanceType), ec2Client, ctx)
}

func FetchRDSInstanceInfo(
	rdsDatabaseIdentifier string,
	rdsClient *rds.Client,
	ctx context.Context,
) (*RDSInstanceInfo, error) {
	args := &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(rdsDatabaseIdentifier)}
	describeDBInstances, err := rdsClient.DescribeDBInstances(ctx, args)
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
	return &RDSInstanceInfo{
		InstanceType: dbInstance,
	}, nil
}

type SystemInfo struct {
	TotalMemoryBytes uint64
	NumCPUs          int
	PGVersion        string
	MaxConnections   int
}

func FetchSystemInfo(
	databaseIdentifier string,
	ec2Client *ec2.Client,
	rdsClient *rds.Client,
	pgPool *pgxpool.Pool,
	ctx context.Context,
) (*SystemInfo, error) {
	rdsInstanceInfo, err := FetchRDSInstanceInfo(databaseIdentifier, rdsClient, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch RDS instance info: %v", err)
	}

	ec2InstanceInfo, err := rdsInstanceInfo.FetchEC2InstanceInfo(ec2Client, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch EC2 instance info: %v", err)
	}

	numCPUs, err := ec2InstanceInfo.VCPUs()
	if err != nil {
		return nil, fmt.Errorf("failed to get CPU count: %v", err)
	}

	totalMemoryBytes, err := ec2InstanceInfo.TotalMemoryBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get total memory: %v", err)
	}

	pgVersion, err := collectors.PGVersion(pgPool)
	if err != nil {
		return nil, err
	}

	maxConnections, err := collectors.MaxConnections(pgPool)
	if err != nil {
		return nil, err
	}

	return &SystemInfo{
		TotalMemoryBytes: totalMemoryBytes,
		NumCPUs:          int(numCPUs),
		PGVersion:        pgVersion,
		MaxConnections:   maxConnections,
	}, nil
}
