package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/collectors"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/spf13/viper"
)

// DockerContainerAdapter works with the container name and by
// communicating with the docker Unix socket to get stats like memory usage,
// number of CPUs available and memory limit
type DockerContainerAdapter struct {
	DefaultPostgreSQLAdapter
	ContainerName string
	dockerClient  *client.Client
}

func CreateDockerContainerAdapter() (*DockerContainerAdapter, error) {
	// Bind environment variables
	viper.BindEnv("docker.container_name", "DBT_DOCKER_CONTAINER_NAME")
	viper.BindEnv("dbtune.database_id", "DBT_DATABASE_ID")

	// Get required configuration
	containerName := viper.GetString("docker.container_name")
	if containerName == "" {
		return nil, fmt.Errorf("docker container name not configured (docker.container_name)")
	}

	defaultAdapter, err := CreateDefaultPostgreSQLAdapter()
	if err != nil {
		return nil, fmt.Errorf("failed to create base PostgreSQL adapter: %w", err)
	}

	// Create Docker client
	cli, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	dockerAdapter := &DockerContainerAdapter{
		DefaultPostgreSQLAdapter: *defaultAdapter,
		ContainerName:            containerName,
		dockerClient:             cli,
	}

	// Override the metrics state to use Docker-specific collectors
	dockerAdapter.MetricsState.Collectors = DockerCollectors(dockerAdapter)

	return dockerAdapter, nil
}

func (d *DockerContainerAdapter) GetContainerName() string {
	return d.ContainerName
}

func (d *DockerContainerAdapter) GetDockerClient() *client.Client {
	return d.dockerClient
}

// DockerCollectors returns the list of collectors for Docker, replacing system metrics
// with Docker-specific ones while keeping database-specific collectors
func DockerCollectors(adapter *DockerContainerAdapter) []agent.MetricCollector {
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
			Collector:  collectors.DockerHardwareInfo(adapter),
		},
		// Use it for testing
		//{
		//	Key:        "failing_slow_queries",
		//	MetricType: "int",
		//	Collector:  collectors.ArtificiallyFailingQueries(pgAdapter),
		//},
	}
}

func (d *DockerContainerAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	d.Logger().Println("Collecting system info for Docker container")

	var systemInfo []utils.FlatValue

	// Get PostgreSQL version using the existing collector
	pgVersion, err := collectors.PGVersion(d)
	if err != nil {
		return nil, err
	}

	// Get max connections from PostgreSQL
	maxConnections, err := collectors.MaxConnections(d)
	if err != nil {
		return nil, err
	}

	// Get container stats
	stats, err := d.dockerClient.ContainerStats(context.Background(), d.ContainerName, false)
	if err != nil {
		return nil, err
	}
	defer stats.Body.Close()

	// Parse stats
	var statsJSON container.StatsResponse
	decoder := json.NewDecoder(stats.Body)
	if err := decoder.Decode(&statsJSON); err != nil {
		return nil, err
	}

	// Get container info for additional details
	containerInfo, err := d.dockerClient.ContainerInspect(context.Background(), d.ContainerName)
	if err != nil {
		return nil, err
	}

	// Create metrics
	version, _ := utils.NewMetric("system_info_pg_version", pgVersion, utils.String)
	maxConnectionsMetric, _ := utils.NewMetric("pg_max_connections", maxConnections, utils.Int)

	// Memory info
	memLimitMetric, _ := utils.NewMetric("node_memory_total", statsJSON.MemoryStats.Limit, utils.Int)

	// CPU info
	noCPUs := float64(len(statsJSON.CPUStats.CPUUsage.PercpuUsage))
	if statsJSON.CPUStats.OnlineCPUs > 0 {
		noCPUs = float64(statsJSON.CPUStats.OnlineCPUs)
	}
	cpuMetric, _ := utils.NewMetric("node_cpu_count", noCPUs, utils.Float)

	// Container info
	containerOS, _ := utils.NewMetric("system_info_os", "linux", utils.String) // Docker containers are Linux-based
	containerPlatform, _ := utils.NewMetric("system_info_platform", "docker", utils.String)
	containerVersion, _ := utils.NewMetric("system_info_platform_version", containerInfo.Config.Image, utils.String)

	systemInfo = append(systemInfo,
		version,
		memLimitMetric,
		cpuMetric,
		maxConnectionsMetric,
		containerOS,
		containerPlatform,
		containerVersion,
	)

	return systemInfo, nil
}

func (d *DockerContainerAdapter) Guardrails() *agent.GuardrailSignal {
	// Get container stats
	stats, err := d.dockerClient.ContainerStats(context.Background(), d.ContainerName, false)
	if err != nil {
		d.Logger().Printf("guardrail: could not fetch docker stats: %v", err)
		return nil
	}
	defer stats.Body.Close()

	// Parse stats
	var statsJSON container.StatsResponse
	decoder := json.NewDecoder(stats.Body)
	if err := decoder.Decode(&statsJSON); err != nil {
		d.Logger().Printf("guardrail: could not decode docker stats: %v", err)
		return nil
	}

	// Check if there's a memory limit
	if statsJSON.MemoryStats.Limit > 0 {
		memoryLimit := statsJSON.MemoryStats.Limit

		memoryUsagePercent := utils.CalculateDockerMemoryUsed(statsJSON.MemoryStats) / float64(memoryLimit) * 100
		d.Logger().Debugf("guardrail: memory percentage is %f", memoryUsagePercent)
		if memoryUsagePercent > d.GuardrailConfig.MemoryThreshold {
			signal := &agent.GuardrailSignal{
				Level: agent.Critical,
				Type:  agent.Memory,
			}
			return signal
		}
	} else {
		d.Logger().Debug("guardrail: could not fetch memory limit")
	}

	return nil
}

func (d *DockerContainerAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	d.Logger().Infof("Applying Config: %s", proposedConfig.KnobApplication)

	// Apply the configuration with ALTER
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return err
		}

		// We make the assumption every setting is a number parsed as float
		query := fmt.Sprintf(`ALTER SYSTEM SET "%s" = %s;`, knobConfig.Name, strconv.FormatFloat(knobConfig.Setting.(float64), 'f', -1, 64))
		d.Logger().Debugf(`Executing: %s`, query)

		_, err = d.pgDriver.Exec(context.Background(), query)
		if err != nil {
			return err
		}
	}

	if proposedConfig.KnobApplication == "restart" {
		// Restart the service
		d.Logger().Warn("Restarting service")

		// Execute docker restart command
		err := d.dockerClient.ContainerRestart(context.Background(), d.ContainerName, container.StopOptions{})
		if err != nil {
			return fmt.Errorf("failed to restart PostgreSQL service: %w", err)
		}

		// Wait for PostgreSQL to be back online with retries
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("timeout waiting for PostgreSQL to come back online")
			case <-time.After(1 * time.Second):
				// Try to execute a simple query
				_, err := d.pgDriver.Exec(ctx, "SELECT 1")
				if err == nil {
					d.Logger().Info("PostgreSQL is back online")
					return nil
				}
				d.Logger().Debug("PostgreSQL not ready yet, retrying...")
			}
		}
	} else {
		// Reload database when everything is applied
		_, err := d.pgDriver.Exec(context.Background(), "SELECT pg_reload_conf();")
		if err != nil {
			return err
		}
	}

	return nil
}
