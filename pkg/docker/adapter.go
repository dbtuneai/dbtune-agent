package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/dbtuneai/agent/pkg/agent"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// DockerContainerAdapter works with the container name and by
// communicating with the docker Unix socket to get stats like memory usage,
// number of CPUs available and memory limit
type DockerContainerAdapter struct {
	agent.CommonAgent
	Config            Config
	dockerClient      *client.Client
	GuardrailSettings guardrails.Config
	PGDriver          *pgxpool.Pool
	PGVersion         string
}

func CreateDockerContainerAdapter() (*DockerContainerAdapter, error) {
	// Get required configuration
	dockerConfig, err := ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for docker %w", err)
	}

	guardrailSettings, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
	}

	// Create Docker client
	cli, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	dbpool, err := pgxpool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	commonAgent := agent.CreateCommonAgent()
	PGVersion, err := pg.PGVersion(dbpool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}
	dockerAdapter := &DockerContainerAdapter{
		CommonAgent:       *commonAgent,
		Config:            dockerConfig,
		dockerClient:      cli,
		GuardrailSettings: guardrailSettings,
		PGDriver:          dbpool,
		PGVersion:         PGVersion,
	}
	collectors := DockerCollectors(dockerAdapter)
	dockerAdapter.InitCollectors(collectors)

	return dockerAdapter, nil
}

// DockerCollectors returns the list of collectors for Docker, replacing system metrics
// with Docker-specific ones while keeping database-specific collectors
func DockerCollectors(adapter *DockerContainerAdapter) []agent.MetricCollector {
	pgDriver := adapter.PGDriver
	collectors := []agent.MetricCollector{
		{
			Key:        "database_average_query_runtime",
			MetricType: "float",
			Collector:  pg.PGStatStatements(pgDriver),
		},
		{
			Key:        "database_transactions_per_second",
			MetricType: "int",
			Collector:  pg.TransactionsPerSecond(pgDriver),
		},
		{
			Key:        "database_active_connections",
			MetricType: "int",
			Collector:  pg.ActiveConnections(pgDriver),
		},
		{
			Key:        "system_db_size",
			MetricType: "int",
			Collector:  pg.DatabaseSize(pgDriver),
		},
		{
			Key:        "database_autovacuum_count",
			MetricType: "int",
			Collector:  pg.Autovacuum(pgDriver),
		},
		{
			Key:        "server_uptime",
			MetricType: "float",
			Collector:  pg.UptimeMinutes(pgDriver),
		},
		{
			Key:        "pg_database",
			MetricType: "int",
			Collector:  pg.PGStatDatabase(pgDriver),
		},
		{
			Key:        "pg_user_tables",
			MetricType: "int",
			Collector:  pg.PGStatUserTables(pgDriver),
		},
		{
			Key:        "pg_bgwriter",
			MetricType: "int",
			Collector:  pg.PGStatBGwriter(pgDriver),
		},
		{
			Key:        "pg_wal",
			MetricType: "int",
			Collector:  pg.PGStatWAL(pgDriver),
		},
		{
			Key:        "database_wait_events",
			MetricType: "int",
			Collector:  pg.WaitEvents(pgDriver),
		},
		{
			Key:        "hardware",
			MetricType: "int",
			Collector:  DockerHardwareInfo(adapter.dockerClient, adapter.Config.ContainerName),
		},
	}
	majorVersion := strings.Split(adapter.PGVersion, ".")
	intMajorVersion, err := strconv.Atoi(majorVersion[0])
	if err != nil {
		adapter.Logger().Warnf("Could not parse major version from version string %s: %v", adapter.PGVersion, err)
		return collectors
	}
	if intMajorVersion >= 17 {
		collectors = append(collectors, agent.MetricCollector{
			Key:        "pg_checkpointer",
			MetricType: "int",
			Collector:  pg.PGStatCheckpointer(pgDriver),
		})
	}
	return collectors
}

func (d *DockerContainerAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
	d.Logger().Println("Collecting system info for Docker container")

	var systemInfo []metrics.FlatValue

	// Get PostgreSQL version using the existing collector
	pgDriver := d.PGDriver
	pgVersion, err := pg.PGVersion(pgDriver)
	if err != nil {
		return nil, err
	}

	// Get max connections from PostgreSQL
	maxConnections, err := pg.MaxConnections(pgDriver)
	if err != nil {
		return nil, err
	}

	// Get container stats
	stats, err := d.dockerClient.ContainerStats(context.Background(), d.Config.ContainerName, false)
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
	containerInfo, err := d.dockerClient.ContainerInspect(context.Background(), d.Config.ContainerName)
	if err != nil {
		return nil, err
	}

	// Create metrics
	version, err := metrics.PGVersion.AsFlatValue(pgVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL version metric: %w", err)
	}

	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to create max connections metric: %w", err)
	}

	// Memory info
	memLimitMetric, err := metrics.NodeMemoryTotal.AsFlatValue(int64(statsJSON.MemoryStats.Limit))
	if err != nil {
		return nil, fmt.Errorf("failed to create memory limit metric: %w", err)
	}

	// CPU info
	var cpuCount float64
	if containerInfo.HostConfig.NanoCPUs > 0 {
		// Convert from nano CPUs to actual CPU count
		cpuCount = float64(containerInfo.HostConfig.NanoCPUs) / 1e9
	} else if containerInfo.HostConfig.CPUQuota > 0 && containerInfo.HostConfig.CPUPeriod > 0 {
		// Convert from quota/period to CPU count
		cpuCount = float64(containerInfo.HostConfig.CPUQuota) / float64(containerInfo.HostConfig.CPUPeriod)
	} else {
		// If no limits set, use the number of CPUs available to the container
		cpuCount = float64(len(statsJSON.CPUStats.CPUUsage.PercpuUsage))
	}
	cpuMetric, err := metrics.NodeCPUCount.AsFlatValue(int64(cpuCount))
	if err != nil {
		return nil, fmt.Errorf("failed to create CPU count metric: %w", err)
	}

	// Container info
	containerOS, err := metrics.NodeOSInfo.AsFlatValue("linux") // Docker containers are Linux-based
	if err != nil {
		return nil, fmt.Errorf("failed to create OS info metric: %w", err)
	}

	containerPlatform, err := metrics.NodeOSPlatform.AsFlatValue("docker")
	if err != nil {
		return nil, fmt.Errorf("failed to create platform metric: %w", err)
	}

	containerVersion, err := metrics.NodeOSPlatformVer.AsFlatValue(containerInfo.Config.Image)
	if err != nil {
		return nil, fmt.Errorf("failed to create platform version metric: %w", err)
	}

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

func (d *DockerContainerAdapter) Guardrails() *guardrails.Signal {
	// Get container stats
	stats, err := d.dockerClient.ContainerStats(context.Background(), d.Config.ContainerName, false)
	if err != nil {
		d.Logger().Warnf("guardrail: could not fetch docker stats: %v", err)
		return nil
	}
	defer stats.Body.Close()

	// Parse stats
	var statsJSON container.StatsResponse
	decoder := json.NewDecoder(stats.Body)
	if err := decoder.Decode(&statsJSON); err != nil {
		d.Logger().Warnf("guardrail: could not decode docker stats: %v", err)
		return nil
	}

	// Check if there's a memory limit
	if statsJSON.MemoryStats.Limit > 0 {
		memoryLimit := statsJSON.MemoryStats.Limit

		memoryUsagePercent := CalculateDockerMemoryUsed(statsJSON.MemoryStats) / float64(memoryLimit) * 100
		d.Logger().Debugf("guardrail: memory percentage is %f", memoryUsagePercent)
		if memoryUsagePercent > d.GuardrailSettings.MemoryThreshold {
			return &guardrails.Signal{
				Level: guardrails.Critical,
				Type:  guardrails.Memory,
			}
		}
	} else {
		d.Logger().Warn("guardrail: could not fetch memory limit")
	}

	return nil
}

func (d *DockerContainerAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	return pg.GetActiveConfig(d.PGDriver, context.Background(), d.Logger())
}

func (d *DockerContainerAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	d.Logger().Infof("Applying Config: %s", proposedConfig.KnobApplication)

	ctx := context.Background()

	parsedKnobs, err := parameters.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return err
	}

	for _, knob := range parsedKnobs {
		err := pg.AlterSystem(d.PGDriver, knob.Name, knob.SettingValue)
		if err != nil {
			return fmt.Errorf("failed to alter system for %s: %w", knob.Name, err)
		}
	}

	if proposedConfig.KnobApplication == "restart" {
		// Restart the service
		d.Logger().Warn("Restarting service")

		// Execute docker restart command
		err := d.dockerClient.ContainerRestart(ctx, d.Config.ContainerName, container.StopOptions{})
		if err != nil {
			return fmt.Errorf("failed to restart PostgreSQL service: %w", err)
		}

		err = pg.WaitPostgresReady(d.PGDriver)
		if err != nil {
			return fmt.Errorf("failed to wait for PostgreSQL to be back online: %w", err)
		}
	} else {
		// Reload database when everything is applied
		err := pg.ReloadConfig(d.PGDriver)
		if err != nil {
			return err
		}
	}

	return nil
}
