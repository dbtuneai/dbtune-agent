package docker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

func (d *DockerContainerAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	d.Logger().Println("Collecting system info for Docker container")

	systemInfo := make([]metrics.FlatValue, 0, 7)

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
	stats, err := d.dockerClient.ContainerStats(ctx, d.Config.ContainerName, client.ContainerStatsOptions{Stream: false})
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
	inspectResult, err := d.dockerClient.ContainerInspect(ctx, d.Config.ContainerName, client.ContainerInspectOptions{})
	if err != nil {
		return nil, err
	}
	containerInfo := inspectResult.Container

	// Create metrics
	version, err := metrics.PGVersion.AsFlatValue(pgVersion.String())
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL version metric: %w", err)
	}

	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to create max connections metric: %w", err)
	}

	// Memory info
	memLimitMetric, err := metrics.NodeMemoryTotal.AsFlatValue(statsJSON.MemoryStats.Limit)
	if err != nil {
		return nil, fmt.Errorf("failed to create memory limit metric: %w", err)
	}

	// CPU info
	cpuCount := CalculateDockerCPUCount(containerInfo.HostConfig, statsJSON.CPUStats)
	cpuMetric, err := metrics.NodeCPUCount.AsFlatValue(max(int64(cpuCount), 1))
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

	databaseInfo, err := pg.DatabaseSystemInfo(pgDriver)
	if err != nil {
		return nil, err
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
	systemInfo = append(systemInfo, databaseInfo...)

	return systemInfo, nil
}
