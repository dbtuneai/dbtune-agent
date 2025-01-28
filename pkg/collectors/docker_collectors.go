package collectors

import (
	"context"
	"encoding/json"

	adapters "github.com/dbtuneai/agent/pkg/adeptersinterfaces"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// DockerHardwareInfo collects hardware metrics from a Docker container using the Docker API
func DockerHardwareInfo(dockerAdapter adapters.DockerAdapter) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		// Create a new Docker client
		cli, err := client.NewClientWithOpts(client.FromEnv)
		if err != nil {
			return err
		}
		defer cli.Close()

		// Get container stats
		containerName := dockerAdapter.GetContainerName()
		stats, err := cli.ContainerStats(ctx, containerName, false)
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
		cpuPercent := utils.CalculateDockerCPUPercent(
			statsJSON.PreCPUStats.CPUUsage.TotalUsage,
			statsJSON.PreCPUStats.SystemUsage,
			&statsJSON,
		)

		// // Calculate memory percentage
		// memoryPercent := float64(statsJSON.MemoryStats.Usage) / float64(statsJSON.MemoryStats.Limit) * 100

		// memPercentMetric, _ := utils.NewMetric("node_memory_usage", memoryPercent, utils.Float)
		// state.AddMetric(memPercentMetric)

		// Add metrics
		cpuMetric, _ := utils.NewMetric("node_cpu_usage", cpuPercent, utils.Float)
		state.AddMetric(cpuMetric)

		memoryUsed := utils.CalculateDockerMemoryUsed(statsJSON.MemoryStats)
		memUsedMetric, _ := utils.NewMetric("node_memory_used", memoryUsed, utils.Float)
		state.AddMetric(memUsedMetric)

		memLimitMetric, _ := utils.NewMetric("node_memory_total", statsJSON.MemoryStats.Limit, utils.Int)
		state.AddMetric(memLimitMetric)

		return nil
	}
}
