package docker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"

	"github.com/docker/docker/api/types/container"
)

// DockerHardwareInfo collects hardware metrics from a Docker container using the Docker API
func DockerHardwareInfo(adapter *DockerContainerAdapter) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		// Resolve container ID (handles both regular and Swarm mode)
		var containerID string
		var err error

		if !adapter.Config.SwarmMode {
			containerID = adapter.Config.ContainerName
		} else {
			// In Swarm mode, we need to find the actual container ID from the service name
			containerID, err = adapter.resolveContainerIDFromNameSubstring(ctx, adapter.Config.ContainerName)
			if err != nil {
				return fmt.Errorf("failed to resolve container ID: %w", err)
			}
		}

		// Get container stats
		stats, err := adapter.dockerClient.ContainerStats(ctx, containerID, false)
		if err != nil {
			return err
		}
		defer stats.Body.Close()

		// Parse stats and create metrics
		var statsJSON container.StatsResponse

		// Decode JSON stats
		decoder := json.NewDecoder(stats.Body)
		if err := decoder.Decode(&statsJSON); err != nil {
			return err
		}

		// Calculate CPU percentage using the utility function
		cpuPercent := CalculateDockerCPUPercent(
			statsJSON.PreCPUStats.CPUUsage.TotalUsage,
			statsJSON.PreCPUStats.SystemUsage,
			&statsJSON,
		)

		// Add metrics
		cpuMetric, err := metrics.NodeCPUUsage.AsFlatValue(cpuPercent)
		if err != nil {
			return err
		}
		state.AddMetric(cpuMetric)

		// Validate memory stats before calculating
		if statsJSON.MemoryStats.Usage == 0 {
			return fmt.Errorf("invalid memory stats: usage is 0")
		}

		memoryUsed := CalculateDockerMemoryUsed(statsJSON.MemoryStats)
		memUsedMetric, err := metrics.NodeMemoryUsed.AsFlatValue(int64(memoryUsed))
		if err != nil {
			return err
		}
		state.AddMetric(memUsedMetric)

		return nil
	}
}
