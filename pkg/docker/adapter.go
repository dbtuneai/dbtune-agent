package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/dbtuneai/agent/pkg/agent"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	pgPool "github.com/jackc/pgx/v5/pgxpool"

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
}

func CreateDockerContainerAdapter() (*DockerContainerAdapter, error) {
	// Get required configuration
	dockerConfig, err := ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for docker %w", err)
	}

	guardrailSettings, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for gaurdrails %w", err)
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

	dbpool, err := pgPool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	commonAgent := agent.CreateCommonAgent()

	dockerAdapter := &DockerContainerAdapter{
		CommonAgent:       *commonAgent,
		Config:            dockerConfig,
		dockerClient:      cli,
		GuardrailSettings: guardrailSettings,
		PGDriver:          dbpool,
	}

	// Override the metrics state to use Docker-specific collectors
	dockerAdapter.MetricsState.Collectors = DockerCollectors(dockerAdapter)

	return dockerAdapter, nil
}

// DockerCollectors returns the list of collectors for Docker, replacing system metrics
// with Docker-specific ones while keeping database-specific collectors
func DockerCollectors(adapter *DockerContainerAdapter) []agent.MetricCollector {
	pgDriver := adapter.PGDriver
	return []agent.MetricCollector{
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
			Collector:  pg.Uptime(pgDriver),
		},
		{
			Key:        "database_cache_hit_ratio",
			MetricType: "float",
			Collector:  pg.BufferCacheHitRatio(pgDriver),
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
		// Use it for testing
		//{
		//	Key:        "failing_slow_queries",
		//	MetricType: "int",
		//	Collector:  pg.ArtificiallyFailingQueries(pgAdapter),
		//},
	}
}

func (d *DockerContainerAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	d.Logger().Println("Collecting system info for Docker container")

	var systemInfo []utils.FlatValue

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

func (d *DockerContainerAdapter) Guardrails() *guardrails.Signal {
	// Get container stats
	stats, err := d.dockerClient.ContainerStats(context.Background(), d.Config.ContainerName, false)
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

		memoryUsagePercent := CalculateDockerMemoryUsed(statsJSON.MemoryStats) / float64(memoryLimit) * 100
		d.Logger().Debugf("guardrail: memory percentage is %f", memoryUsagePercent)
		if memoryUsagePercent > d.GuardrailSettings.MemoryThreshold {
			return &guardrails.Signal{
				Level: guardrails.Critical,
				Type:  guardrails.Memory,
			}
		}
	} else {
		d.Logger().Debug("guardrail: could not fetch memory limit")
	}

	return nil
}

func (d *DockerContainerAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	return pg.GetActiveConfig(d.PGDriver, context.Background(), d.Logger())
}

func (d *DockerContainerAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	d.Logger().Infof("Applying Config: %s", proposedConfig.KnobApplication)

	ctx := context.Background()
	// Apply the configuration with ALTER
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return err
		}

		// We make the assumption every setting is a number parsed as float
		err = pg.AlterSystem(d.PGDriver, knobConfig.Name, strconv.FormatFloat(knobConfig.Setting.(float64), 'f', -1, 64))
		if err != nil {
			return fmt.Errorf("failed to alter system: %w", err)
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

		// Wait for PostgreSQL to be back online with retries
		pg.WaitPostgresReady(d.PGDriver, ctx)
	} else {
		// Reload database when everything is applied
		err := pg.ReloadConfig(d.PGDriver)
		if err != nil {
			return err
		}
	}

	return nil
}
