package adapters

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/pi"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	adapters "github.com/dbtuneai/agent/pkg/adeptersinterfaces"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/collectors"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/hashicorp/go-retryablehttp"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
)

type AuroraRDSAdapter struct {
	DefaultPostgreSQLAdapter
	auroraConfig     adapters.AuroraRDSConfig
	rdsClient        *rds.Client
	cloudwatchClient *cloudwatch.Client
	piClient         *pi.Client
	ec2Client        *ec2.Client
	state            *adapters.AuroraRDSState
}

func (adapter *AuroraRDSAdapter) PGDriver() *pgPool.Pool {
	return adapter.pgDriver
}

func (adapter *AuroraRDSAdapter) APIClient() *retryablehttp.Client {
	return adapter.APIClient()
}

func (adapter *AuroraRDSAdapter) GetAuroraState() *adapters.AuroraRDSState {
	return adapter.state
}

func (adapter *AuroraRDSAdapter) GetAuroraRDSConfig() *adapters.AuroraRDSConfig {
	return &adapter.auroraConfig
}

func (adapter *AuroraRDSAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	adapter.Logger().Println("Collecting system info")

	var systemInfo []utils.FlatValue

	instanceTypeInfo, err := adapter.GetEC2InstanceTypeInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get instance type info: %v", err)
	}

	// Get CPU count from instance type info
	numCPUs := instanceTypeInfo.VCpuInfo.DefaultVCpus
	if numCPUs == nil {
		return nil, fmt.Errorf("CPU information not available")
	}

	// Get total memory from instance type info
	memInfo := instanceTypeInfo.MemoryInfo
	if memInfo == nil || memInfo.SizeInMiB == nil {
		return nil, fmt.Errorf("memory information not available")
	}
	totalMemoryBytes := *memInfo.SizeInMiB * 1024 * 1024 // Convert MiB to bytes

	// Update hardware state
	adapter.state.Hardware = &adapters.AuroraHardwareState{
		TotalMemoryBytes: totalMemoryBytes,
		NumCPUs:          int(*numCPUs),
		LastChecked:      time.Now(),
	}

	totalMemory, err := utils.NewMetric("node_memory_total", totalMemoryBytes, utils.Int)
	if err != nil {
		return nil, err
	}

	noCPUsMetric, err := utils.NewMetric("node_cpu_count", int(*numCPUs), utils.Int)
	if err != nil {
		return nil, err
	}

	pgVersion, err := collectors.PGVersion(adapter)
	if err != nil {
		return nil, err
	}

	maxConnections, err := collectors.MaxConnections(adapter)
	if err != nil {
		return nil, err
	}

	version, _ := utils.NewMetric("system_info_pg_version", pgVersion, utils.String)
	maxConnectionsMetric, _ := utils.NewMetric("pg_max_connections", maxConnections, utils.Int)

	systemInfo = append(systemInfo, version, totalMemory, maxConnectionsMetric, noCPUsMetric)

	// For RDS Aurora, disk type is always SSD
	diskTypeMetric, _ := utils.NewMetric("node_disk_device_type", "SSD", utils.String)
	systemInfo = append(systemInfo, diskTypeMetric)

	return systemInfo, nil
}

func (adapter *AuroraRDSAdapter) GetPIClient() *pi.Client {
	return adapter.piClient
}

func (adapter *AuroraRDSAdapter) GetEC2Client() *ec2.Client {
	return adapter.ec2Client
}

func (adapter *AuroraRDSAdapter) GetRDSClient() *rds.Client {
	return adapter.rdsClient
}

func (adapter *AuroraRDSAdapter) GetCWClient() *cloudwatch.Client {
	return adapter.cloudwatchClient
}

func (adapter *AuroraRDSAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	// If the last applied config is less than 1 minute ago, return
	if adapter.state.LastAppliedConfig.Add(1 * time.Minute).After(time.Now()) {
		adapter.Logger().Info("Last applied config is less than 1 minute ago, skipping")
		return nil
	}

	adapter.Logger().Infof("Applying Config: %s", proposedConfig.KnobApplication)

	// Prepare parameters for modification
	var modifiedParameters []rdstypes.Parameter
	var applyMethod rdstypes.ApplyMethod

	if proposedConfig.KnobApplication == "restart" {
		applyMethod = rdstypes.ApplyMethodPendingReboot
	} else {
		applyMethod = rdstypes.ApplyMethodImmediate
	}

	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return fmt.Errorf("failed to find recommended knob: %v", err)
		}
		fmtValue, err := knobConfig.GetSettingValue()
		if err != nil {
			return fmt.Errorf("failed to get setting value: %v", err)
		}

		param := rdstypes.Parameter{
			ParameterName:  aws.String(knobConfig.Name),
			ParameterValue: aws.String(fmtValue),
			ApplyMethod:    applyMethod,
		}
		modifiedParameters = append(modifiedParameters, param)
	}

	// Modify parameter group
	_, err := adapter.rdsClient.ModifyDBParameterGroup(context.Background(), &rds.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String(adapter.auroraConfig.RDSParameterGroupName),
		Parameters:           modifiedParameters,
	})
	if err != nil {
		return fmt.Errorf("failed to modify parameter group: %v", err)
	}

	// Wait for parameter group changes to be processed
	adapter.Logger().Info("Waiting for parameter group changes to be processed...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Wait for the parameter apply status to be either pending-reboot or in-sync
	paramGroupReady := false
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for parameter group changes to be processed")
		case <-time.After(5 * time.Second):
			output, err := adapter.rdsClient.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(adapter.auroraConfig.RDSDatabaseIdentifier),
			})
			if err != nil {
				adapter.Logger().Warnf("Failed to check parameter apply status: %v", err)
				continue
			}

			if len(output.DBInstances) == 0 {
				adapter.Logger().Warn("instance not found while checking parameter apply status")
				continue
			}

			// Get the parameter group status
			if len(output.DBInstances[0].DBParameterGroups) > 0 {
				status := output.DBInstances[0].DBParameterGroups[0].ParameterApplyStatus
				adapter.Logger().Infof("Current parameter apply status: %s", *status)

				// Check if we've reached a terminal state
				if applyMethod == rdstypes.ApplyMethodPendingReboot {
					if *status == "pending-reboot" {
						adapter.Logger().Info("Parameter group changes have been processed")
						paramGroupReady = true
						break
					}
				} else {
					if *status == "in-sync" {
						adapter.Logger().Info("Parameter group changes have been processed")
						paramGroupReady = true
						break
					}
				}
			}
		}

		if paramGroupReady {
			break
		}
	}

	// If restart is required and specified
	if proposedConfig.KnobApplication == "restart" {
		_, err = adapter.rdsClient.RebootDBInstance(context.Background(), &rds.RebootDBInstanceInput{
			DBInstanceIdentifier: aws.String(adapter.auroraConfig.RDSDatabaseIdentifier),
		})
		if err != nil {
			return fmt.Errorf("failed to reboot RDS instance: %v", err)
		}
	}

	// Wait for the instance to become available and PostgreSQL to be online
	for {
		output, err := adapter.rdsClient.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(adapter.auroraConfig.RDSDatabaseIdentifier),
		})
		if err != nil {
			return fmt.Errorf("failed to check instance status: %v", err)
		}

		if len(output.DBInstances) == 0 {
			return fmt.Errorf("instance not found")
		}

		status := output.DBInstances[0].DBInstanceStatus
		if *status == "available" {
			adapter.Logger().Info("Database instance is available")
			break
		}
		adapter.Logger().Warnf("Database instance is not available yet. Status is:  %s - retrying...", *status)
		time.Sleep(5 * time.Second)
	}

	// TODO: validate if this is the case or we were not waiting properly for parameter group changes
	//Aurora has a race-condition/caching issue where the first fetches of config
	// after a restart are giving back the old config.
	// This results in re-applying the old recommended config.
	// To avoid this, we give a buffer of last applied config of 1 minute.

	// Instance is online, we validate that PostgreSQL is back online also
	ctx, cancel = context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for PostgreSQL to come back online")
		case <-time.After(1 * time.Second):
			// Try to execute a simple query
			_, err := adapter.pgDriver.Exec(ctx, "SELECT 1")
			if err == nil {
				adapter.Logger().Info("PostgreSQL is back online")
				adapter.state.LastAppliedConfig = time.Now()
				return nil
			}
			adapter.Logger().Debug("PostgreSQL not ready yet, retrying...")
		}
	}
}

func AuroraCollectors(adapter *AuroraRDSAdapter) []agent.MetricCollector {
	return []agent.MetricCollector{
		{
			Key:        "database_average_query_runtime",
			MetricType: "float",
			Collector:  collectors.PGStatStatements(adapter),
		},
		{
			Key:        "database_transactions_per_second",
			MetricType: "int",
			Collector:  collectors.TransactionsPerSecond(adapter),
		},
		{
			Key:        "database_active_connections",
			MetricType: "int",
			Collector:  collectors.ActiveConnections(adapter),
		},
		{
			Key:        "system_db_size",
			MetricType: "int",
			Collector:  collectors.DatabaseSize(adapter),
		},
		{
			Key:        "database_autovacuum_count",
			MetricType: "int",
			Collector:  collectors.Autovacuum(adapter),
		},
		{
			Key:        "server_uptime",
			MetricType: "float",
			Collector:  collectors.Uptime(adapter),
		},
		{
			Key:        "database_cache_hit_ratio",
			MetricType: "float",
			Collector:  collectors.BufferCacheHitRatio(adapter),
		},
		{
			Key:        "database_wait_events",
			MetricType: "int",
			Collector:  collectors.WaitEvents(adapter),
		},
		{
			Key:        "hardware",
			MetricType: "int",
			Collector:  collectors.AuroraHardwareInfo(adapter),
		},
	}
}

func CreateAuroraRDSAdapter() (*AuroraRDSAdapter, error) {
	// Create a new Viper instance for Aurora configuration if the sub-config doesn't exist
	dbtuneConfig := viper.Sub("rds-aurora")
	if dbtuneConfig == nil {
		// If the section doesn't exist in the config file, create a new Viper instance
		dbtuneConfig = viper.New()
	}

	// Bind environment variables directly to keys (not nested paths)
	dbtuneConfig.BindEnv("AWS_ACCESS_KEY_ID", "DBT_AWS_ACCESS_KEY_ID")
	dbtuneConfig.BindEnv("AWS_SECRET_ACCESS_KEY", "DBT_AWS_SECRET_ACCESS_KEY")
	dbtuneConfig.BindEnv("AWS_REGION", "DBT_AWS_REGION")
	dbtuneConfig.BindEnv("RDS_PARAMETER_GROUP_NAME", "DBT_RDS_PARAMETER_GROUP_NAME")
	dbtuneConfig.BindEnv("RDS_DATABASE_IDENTIFIER", "DBT_RDS_DATABASE_IDENTIFIER")

	// Bind also global environment variables as fallback for AWS credentials
	dbtuneConfig.BindEnv("AWS_ACCESS_KEY_ID")
	dbtuneConfig.BindEnv("AWS_SECRET_ACCESS_KEY")
	dbtuneConfig.BindEnv("AWS_REGION")

	var auroraConfig adapters.AuroraRDSConfig
	err := dbtuneConfig.Unmarshal(&auroraConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %v", err)
	}

	// Validate required configuration
	err = utils.ValidateStruct(&auroraConfig)
	if err != nil {
		return nil, err
	}

	// Validate non-credential required configuration
	if auroraConfig.AWSRegion == "" {
		return nil, fmt.Errorf("AWS_REGION is required")
	}
	if auroraConfig.RDSDatabaseIdentifier == "" {
		return nil, fmt.Errorf("RDS_DATABASE_IDENTIFIER is required")
	}
	if auroraConfig.RDSParameterGroupName == "" {
		return nil, fmt.Errorf("RDS_PARAMETER_GROUP_NAME is required")
	}

	// Create AWS config
	var cfg aws.Config

	if auroraConfig.AWSAccessKey != "" && auroraConfig.AWSSecretAccessKey != "" {
		// Use static credentials if provided
		cfg, err = config.LoadDefaultConfig(context.Background(),
			config.WithRegion(auroraConfig.AWSRegion),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				auroraConfig.AWSAccessKey,
				auroraConfig.AWSSecretAccessKey,
				"",
			)),
		)
	} else {
		// Use default credential chain
		// Includes by default WebIdentityToken:
		// https://github.com/aws/aws-sdk-go-v2/blob/main/config/resolve_credentials.go#L119
		cfg, err = config.LoadDefaultConfig(context.Background(),
			config.WithRegion(auroraConfig.AWSRegion),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("unable to load AWS config: %v", err)
	}

	// Create RDS client
	rdsClient := rds.NewFromConfig(cfg)

	// Check if RDS client can fetch the database instance correctly and the tokens work
	describeDBInstances, err := rdsClient.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(auroraConfig.RDSDatabaseIdentifier),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe database instance: %w", err)
	}

	if len(describeDBInstances.DBInstances) == 0 {
		return nil, fmt.Errorf("database instance not found")
	}

	// Create CloudWatch client
	cloudwatchClient := cloudwatch.NewFromConfig(cfg)

	// Create PI client
	piClient := pi.NewFromConfig(cfg)

	defaultAdapter, err := CreateDefaultPostgreSQLAdapter()
	if err != nil {
		return nil, fmt.Errorf("failed to create base PostgreSQL adapter: %w", err)
	}

	// Get EC2 client
	ec2Client := ec2.NewFromConfig(cfg)

	c := &AuroraRDSAdapter{
		DefaultPostgreSQLAdapter: *defaultAdapter,
		auroraConfig:             auroraConfig,
		rdsClient:                rdsClient,
		cloudwatchClient:         cloudwatchClient,
		piClient:                 piClient,
		ec2Client:                ec2Client,
		state: &adapters.AuroraRDSState{
			LastAppliedConfig: time.Time{},
		},
	}

	// Initialize collectors with AuroraCollectors instead of DefaultCollectors
	c.MetricsState = agent.MetricsState{
		Collectors: AuroraCollectors(c),
		Cache:      agent.Caches{},
		Mutex:      &sync.Mutex{},
	}

	return c, nil
}

// Guardrails checks memory utilization and returns Critical if thresholds are exceeded
func (adapter *AuroraRDSAdapter) Guardrails() *agent.GuardrailSignal {
	if time.Since(adapter.state.LastGuardrailCheck) < 5*time.Second {
		return nil
	}
	adapter.Logger().Info("Checking guardrails")

	// Update Performance Insights status if needed
	if err := adapter.updatePerformanceInsightsStatus(); err != nil {
		adapter.Logger().Errorf("Failed to update Performance Insights status: %v", err)
		return nil
	}

	if adapter.state.Hardware == nil {
		adapter.Logger().Warn("hardware information not available, skipping guardrails")
		return nil
	}

	adapter.state.LastGuardrailCheck = time.Now()
	if adapter.state.PerformanceInsights.Enabled {
		memoryUsageBytes, err := collectors.GetMemoryUsageFromPI(adapter)
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage from PI: %v", err)
			return nil
		}

		memoryUsagePercent := (float64(memoryUsageBytes) / float64(adapter.state.Hardware.TotalMemoryBytes)) * 100

		adapter.Logger().Warnf("Memory usage: %.2f%%", memoryUsagePercent)
		if memoryUsagePercent > adapter.GuardrailConfig.MemoryThreshold {
			signal := &agent.GuardrailSignal{
				Level: agent.Critical,
				Type:  agent.Memory,
			}
			return signal
		}

	} else {
		freeableMemoryBytes, err := collectors.GetFreeableMemoryFromCW(adapter)
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage from CloudWatch: %v", err)
			return nil
		}
		freeableMemoryPercent := (float64(freeableMemoryBytes) / float64(adapter.state.Hardware.TotalMemoryBytes)) * 100

		adapter.Logger().Warnf("Freeable memory: %.2f%%", freeableMemoryPercent)
		if freeableMemoryPercent < (100 - adapter.GuardrailConfig.MemoryThreshold) {
			signal := &agent.GuardrailSignal{
				Level: agent.Critical,
				Type:  agent.Memory,
			}
			return signal
		}
	}

	return nil
}

// updatePerformanceInsightsStatus checks and updates the PI status in the state
// Will check every 2 minutes
func (adapter *AuroraRDSAdapter) updatePerformanceInsightsStatus() error {
	if adapter.state.PerformanceInsights != nil &&
		time.Since(adapter.state.PerformanceInsights.LastChecked) < 2*time.Minute {
		return nil
	}

	describeDBInstances, err := adapter.rdsClient.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(adapter.auroraConfig.RDSDatabaseIdentifier),
	})
	if err != nil {
		return fmt.Errorf("failed to describe database instance: %v", err)
	}

	if len(describeDBInstances.DBInstances) == 0 {
		return fmt.Errorf("instance not found")
	}

	piEnabled := false
	if describeDBInstances.DBInstances[0].PerformanceInsightsEnabled != nil {
		piEnabled = *describeDBInstances.DBInstances[0].PerformanceInsightsEnabled
	}

	// Get the ARN from the instance
	resourceID := ""
	if describeDBInstances.DBInstances[0].PerformanceInsightsEnabled != nil &&
		*describeDBInstances.DBInstances[0].PerformanceInsightsEnabled {
		resourceID = *describeDBInstances.DBInstances[0].DbiResourceId
	}

	adapter.state.PerformanceInsights = &adapters.AuroraPerformanceInsightsState{
		Enabled:     piEnabled,
		ResourceID:  resourceID,
		LastChecked: time.Now(),
	}

	return nil
}

func (adapter *AuroraRDSAdapter) GetEC2InstanceTypeInfo() (*ec2types.InstanceTypeInfo, error) {

	describeDBInstances, err := adapter.rdsClient.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(adapter.auroraConfig.RDSDatabaseIdentifier),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to describe database instance: %v", err)
	}

	if len(describeDBInstances.DBInstances) == 0 {
		return nil, fmt.Errorf("instance not found")
	}

	instanceClass := describeDBInstances.DBInstances[0].DBInstanceClass

	// Convert RDS instance class to EC2 instance type (remove "db." prefix)
	instanceType := strings.TrimPrefix(*instanceClass, "db.")

	// Get instance type information
	describeInstanceTypes, err := adapter.ec2Client.DescribeInstanceTypes(context.Background(), &ec2.DescribeInstanceTypesInput{
		InstanceTypes: []ec2types.InstanceType{ec2types.InstanceType(instanceType)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe instance type: %v", err)
	}

	if len(describeInstanceTypes.InstanceTypes) == 0 {
		return nil, fmt.Errorf("instance type information not found")
	}

	return &describeInstanceTypes.InstanceTypes[0], nil
}
