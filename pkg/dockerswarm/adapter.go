package dockerswarm

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/client"
)

// DockerContainerAdapter works with the service name and by
// communicating with the docker Unix socket to get stats like memory usage,
// number of CPUs available and memory limit from service tasks
type DockerContainerAdapter struct {
	agent.CommonAgent
	Config            Config
	dockerClient      *client.Client
	GuardrailSettings guardrails.Config
	pgConfig          pg.Config
	PGDriver          *pgxpool.Pool
	PGVersion         string
}

func CreateDockerSwarmAdapter() (*DockerContainerAdapter, error) {
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
		pgConfig:          pgConfig,
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
			Key:       "database_average_query_runtime",
			Collector: pg.PGStatStatements(pgDriver, adapter.pgConfig.IncludeQueries, adapter.pgConfig.MaximumQueryTextLength),
		},
		{
			Key:       "database_transactions_per_second",
			Collector: pg.TransactionsPerSecond(pgDriver),
		},
		{
			Key:       "database_connections",
			Collector: pg.Connections(pgDriver),
		},
		{
			Key:       "system_db_size",
			Collector: pg.DatabaseSize(pgDriver),
		},
		{
			Key:       "database_autovacuum_count",
			Collector: pg.Autovacuum(pgDriver),
		},
		{
			Key:       "server_uptime",
			Collector: pg.UptimeMinutes(pgDriver),
		},
		{
			Key:       "pg_database",
			Collector: pg.PGStatDatabase(pgDriver),
		},
		{
			Key:       "pg_user_tables",
			Collector: pg.PGStatUserTables(pgDriver),
		},
		{
			Key:       "pg_bgwriter",
			Collector: pg.PGStatBGwriter(pgDriver),
		},
		{
			Key:       "pg_wal",
			Collector: pg.PGStatWAL(pgDriver),
		},
		{
			Key:       "database_wait_events",
			Collector: pg.WaitEvents(pgDriver),
		},
		{
			Key:       "hardware",
			Collector: DockerHardwareInfo(adapter.dockerClient, adapter.Config.ServiceName),
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
			Key:       "pg_checkpointer",
			Collector: pg.PGStatCheckpointer(pgDriver),
		})
	}
	return collectors
}

// getRunningTaskContainer returns the container ID of a running task for the service
func (d *DockerContainerAdapter) getRunningTaskContainer(ctx context.Context) (string, error) {
	// Create filter for this service
	filter := filters.NewArgs()
	filter.Add("service", d.Config.ServiceName)
	filter.Add("desired-state", "running")

	// List tasks for this service
	tasks, err := d.dockerClient.TaskList(ctx, types.TaskListOptions{
		Filters: filter,
	})
	if err != nil {
		return "", fmt.Errorf("failed to list tasks for service %s: %w", d.Config.ServiceName, err)
	}

	// Find the first running task
	for _, task := range tasks {
		if task.Status.State == swarm.TaskStateRunning && task.Status.ContainerStatus != nil {
			return task.Status.ContainerStatus.ContainerID, nil
		}
	}

	return "", fmt.Errorf("no running tasks found for service %s", d.Config.ServiceName)
}

func (d *DockerContainerAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
	d.Logger().Println("Collecting system info for Docker service")

	var systemInfo []metrics.FlatValue
	ctx := context.Background()

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

	// Get service info
	service, _, err := d.dockerClient.ServiceInspectWithRaw(ctx, d.Config.ServiceName, types.ServiceInspectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to inspect service %s: %w", d.Config.ServiceName, err)
	}

	// Get a running task container ID
	containerID, err := d.getRunningTaskContainer(ctx)
	if err != nil {
		return nil, err
	}

	// Get container stats from the running task
	stats, err := d.dockerClient.ContainerStats(ctx, containerID, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get stats for task container %s: %w", containerID, err)
	}
	defer stats.Body.Close()

	// Parse stats
	var statsJSON container.StatsResponse
	decoder := json.NewDecoder(stats.Body)
	if err := decoder.Decode(&statsJSON); err != nil {
		return nil, err
	}

	// Get container info for additional details
	containerInfo, err := d.dockerClient.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect task container %s: %w", containerID, err)
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

	// CPU info - check service resources first, then container limits
	var cpuCount float64
	if service.Spec.TaskTemplate.Resources != nil && service.Spec.TaskTemplate.Resources.Limits != nil && service.Spec.TaskTemplate.Resources.Limits.NanoCPUs > 0 {
		// Use service-level CPU limits (preferred for swarm)
		cpuCount = float64(service.Spec.TaskTemplate.Resources.Limits.NanoCPUs) / 1e9
	} else if containerInfo.HostConfig.NanoCPUs > 0 {
		// Convert from nano CPUs to actual CPU count
		cpuCount = float64(containerInfo.HostConfig.NanoCPUs) / 1e9
	} else if containerInfo.HostConfig.CPUQuota > 0 && containerInfo.HostConfig.CPUPeriod > 0 {
		// Convert from quota/period to CPU count
		cpuCount = float64(containerInfo.HostConfig.CPUQuota) / float64(containerInfo.HostConfig.CPUPeriod)
	} else {
		// If no limits set, try to get CPU count from online CPUs or per-CPU usage array
		if len(statsJSON.CPUStats.CPUUsage.PercpuUsage) > 0 {
			cpuCount = float64(len(statsJSON.CPUStats.CPUUsage.PercpuUsage))
		} else {
			// Fallback: try to get from system info or use a reasonable default
			// In Docker, if we can't determine CPU count, we might be in a constrained environment
			// Let's inspect the host to get the actual CPU count
			hostInfo, err := d.dockerClient.Info(context.Background())
			if err == nil && hostInfo.NCPU > 0 {
				cpuCount = float64(hostInfo.NCPU)
				d.Logger().Infof("Using host CPU count as fallback: %d CPUs", hostInfo.NCPU)
			} else {
				// Final fallback - assume single CPU if we can't determine
				cpuCount = 0.0
				d.Logger().Warnf("Could not determine CPU count, defaulting to 0")
			}
		}
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

	// Use service image specification (preferred for swarm)
	serviceImage := service.Spec.TaskTemplate.ContainerSpec.Image
	containerVersion, err := metrics.NodeOSPlatformVer.AsFlatValue(serviceImage)
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
	ctx := context.Background()

	// Get a running task container ID
	containerID, err := d.getRunningTaskContainer(ctx)
	if err != nil {
		d.Logger().Warnf("guardrail: could not get running task: %v", err)
		return nil
	}

	// Get container stats from the running task
	stats, err := d.dockerClient.ContainerStats(ctx, containerID, false)
	if err != nil {
		d.Logger().Warnf("guardrail: could not fetch docker stats for task: %v", err)
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

	// Wait 1 minute after applying all configuration changes
	d.Logger().Info("Waiting 1 minute for configuration changes to take effect")
	time.Sleep(1 * time.Minute)

	if proposedConfig.KnobApplication == "restart" {
		// Restart the service by forcing an update
		d.Logger().Warn("Restarting service")

		// Get current service spec
		service, _, err := d.dockerClient.ServiceInspectWithRaw(ctx, d.Config.ServiceName, types.ServiceInspectOptions{})
		if err != nil {
			return fmt.Errorf("failed to inspect service %s: %w", d.Config.ServiceName, err)
		}

		// Force update the service to restart it
		service.Spec.TaskTemplate.ForceUpdate++
		_, err = d.dockerClient.ServiceUpdate(
			ctx,
			d.Config.ServiceName,
			service.Version,
			service.Spec,
			types.ServiceUpdateOptions{},
		)
		if err != nil {
			return fmt.Errorf("failed to restart service %s: %w", d.Config.ServiceName, err)
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
