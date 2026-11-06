package docker

import (
	"context"
	"encoding/json"

	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

func (d *DockerContainerAdapter) Guardrails(ctx context.Context) *guardrails.Signal {
	// Get container stats
	stats, err := d.dockerClient.ContainerStats(ctx, d.Config.ContainerName, client.ContainerStatsOptions{Stream: false})
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
