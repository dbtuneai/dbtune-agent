package kubernetes

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
)

// ContainerMetricsConfig holds configuration for collecting container metrics
type ContainerMetricsConfig struct {
	Client    *Client
	Namespace string
}

// ContainerMetricsCache stores previous metric values for calculating deltas
type ContainerMetricsCache struct {
	PreviousCPUMillicores       int64
	PreviousMemoryBytes         int64
	PreviousReadBytesTotal      uint64
	PreviousWriteBytesTotal     uint64
	PreviousReadsTotal          uint64
	PreviousWritesTotal         uint64
	PreviousNetworkReceiveBytes uint64
	PreviousNetworkSendBytes    uint64
	PreviousTimestamp           time.Time
	PreviousCAdvisorTimestamp   int64 // Timestamp from cAdvisor metrics to detect stale data
	Initialized                 bool
}

// ContainerMetricsCollector returns a collector function that gathers container metrics from Kubernetes
func ContainerMetricsCollector(client Client, podName string, containerName string) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		containerClient := client.ContainerClient(podName, containerName)
		return CollectContainerMetrics(ctx, containerClient, state)
	}
}

// CollectContainerMetrics gathers container metrics from Kubernetes
// It collects CPU and memory usage, as well as I/O statistics, calculating deltas where appropriate
func CollectContainerMetrics(ctx context.Context, containerClient ContainerClient, state *agent.MetricsState) error {
	// Initialize cache in the state if not already present
	if state.Cache.HardwareCache == nil {
		state.Cache.HardwareCache = make(map[string]any)
	}

	// Get or create the container metrics cache
	var cache ContainerMetricsCache
	if cachedData, exists := state.Cache.HardwareCache["container_metrics"]; exists {
		if c, ok := cachedData.(ContainerMetricsCache); ok {
			cache = c
		}
	}

	// Collect CPU metrics
	currentTimestamp := time.Now()
	cpuMillicores, err := containerClient.CPUUsageMillicores(ctx)
	if err != nil {
		return fmt.Errorf("failed to get CPU usage: %w", err)
	}

	cpuLimitMillicores, err := containerClient.CPULimitCoresMilli(ctx)
	if err != nil {
		return fmt.Errorf("failed to get CPU limit: %w", err)
	}

	// Calculate CPU usage percentage
	// Note: CPUUsageMillicores returns instantaneous usage from metrics-server, not cumulative
	var cpuUsagePercent float64
	if cpuLimitMillicores > 0 {
		cpuUsagePercent = (float64(cpuMillicores) / float64(cpuLimitMillicores)) * 100.0

		// Clamp to 0-100 range
		if cpuUsagePercent < 0 {
			cpuUsagePercent = 0
		}
		if cpuUsagePercent > 100 {
			cpuUsagePercent = 100
		}
	}

	// Add CPU usage metric
	cpuUsageMetric, err := metrics.NodeCPUUsage.AsFlatValue(cpuUsagePercent)
	if err == nil {
		state.AddMetric(cpuUsageMetric)
	}

	// Note: CPU count is now collected in PodSystemInfoCollector as it's static

	// Collect memory metrics
	memoryUsageBytes, err := containerClient.MemoryUsageBytes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get memory usage: %w", err)
	}

	memoryLimitBytes, err := containerClient.MemoryLimitBytes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get memory limit: %w", err)
	}

	// Add memory metrics (used only, total is in system info)
	memUsedMetric, err := metrics.NodeMemoryUsed.AsFlatValue(memoryUsageBytes)
	if err != nil {
		return fmt.Errorf("failed to create memory used metric: %w", err)
	}
	state.AddMetric(memUsedMetric)

	// Note: Memory total is now collected in PodSystemInfoCollector as it's static

	// Calculate memory usage percentage
	var memUsagePercent float64
	if memoryLimitBytes > 0 {
		memUsagePercent = (float64(memoryUsageBytes) / float64(memoryLimitBytes)) * 100.0
	}
	memUsagePercentMetric, err := metrics.NodeMemoryUsedPercentage.AsFlatValue(memUsagePercent)
	if err != nil {
		return fmt.Errorf("failed to create memory usage percentage metric: %w", err)
	}
	state.AddMetric(memUsagePercentMetric)

	// Collect I/O stats
	ioStats, err := containerClient.IOStats(ctx)
	if err != nil {
		// Might be blank on first pass
	} else if cache.Initialized && !cache.PreviousTimestamp.IsZero() {
		// Check if cAdvisor metrics have been updated since last collection
		// If timestamp hasn't changed, skip reporting to avoid zero deltas from stale data
		if ioStats.Timestamp != 0 && ioStats.Timestamp == cache.PreviousCAdvisorTimestamp {
			// Metrics haven't been updated by cAdvisor yet, skip this collection
		} else {
			// Calculate deltas for I/O operations (only if we have previous data)
			readsDelta := ioStats.ReadsTotal - cache.PreviousReadsTotal
			writesDelta := ioStats.WritesTotal - cache.PreviousWritesTotal

			// Add I/O count metrics (delta since last collection)
			readCountMetric, err := metrics.NodeDiskIOReadCount.AsFlatValue(readsDelta)
			if err == nil {
				state.AddMetric(readCountMetric)
			}

			writeCountMetric, err := metrics.NodeDiskIOWriteCount.AsFlatValue(writesDelta)
			if err == nil {
				state.AddMetric(writeCountMetric)
			}

			totalIOMetric, err := metrics.NodeDiskIOTotalCount.AsFlatValue(readsDelta + writesDelta)
			if err == nil {
				state.AddMetric(totalIOMetric)
			}

			// Calculate IOPS per second if we have time delta
			timeDelta := currentTimestamp.Sub(cache.PreviousTimestamp).Seconds()
			if timeDelta > 0 {
				readsPerSecond := float64(readsDelta) / timeDelta
				writesPerSecond := float64(writesDelta) / timeDelta
				totalPerSecond := float64(readsDelta+writesDelta) / timeDelta

				// Add IOPS per-second metrics
				if readIOPSMetric, err := metrics.NodeDiskIOPSReadPerSecond.AsFlatValue(readsPerSecond); err == nil {
					state.AddMetric(readIOPSMetric)
				}

				if writeIOPSMetric, err := metrics.NodeDiskIOPSWritePerSecond.AsFlatValue(writesPerSecond); err == nil {
					state.AddMetric(writeIOPSMetric)
				}

				if totalIOPSMetric, err := metrics.NodeDiskIOPSTotalPerSecond.AsFlatValue(totalPerSecond); err == nil {
					state.AddMetric(totalIOPSMetric)
				}
			}

			// Update cache with current I/O values and timestamp
			cache.PreviousReadsTotal = ioStats.ReadsTotal
			cache.PreviousWritesTotal = ioStats.WritesTotal
			cache.PreviousReadBytesTotal = ioStats.ReadBytesTotal
			cache.PreviousWriteBytesTotal = ioStats.WriteBytesTotal
			cache.PreviousCAdvisorTimestamp = ioStats.Timestamp
		}
	} else {
		// First run - just store the values for next time
		cache.PreviousReadsTotal = ioStats.ReadsTotal
		cache.PreviousWritesTotal = ioStats.WritesTotal
		cache.PreviousReadBytesTotal = ioStats.ReadBytesTotal
		cache.PreviousWriteBytesTotal = ioStats.WriteBytesTotal
		cache.PreviousCAdvisorTimestamp = ioStats.Timestamp
	}

	// Collect network stats
	networkStats, err := containerClient.NetworkStats(ctx)
	if err != nil {
		// Network stats might not be available
	} else if cache.Initialized && !cache.PreviousTimestamp.IsZero() {
		// Calculate deltas for network operations
		receiveDelta := networkStats.ReceiveBytesTotal - cache.PreviousNetworkReceiveBytes
		sendDelta := networkStats.TransmitBytesTotal - cache.PreviousNetworkSendBytes

		// Add network count metrics (delta since last collection)
		if receiveCountMetric, err := metrics.NodeNetworkReceiveCount.AsFlatValue(receiveDelta); err == nil {
			state.AddMetric(receiveCountMetric)
		}

		if sendCountMetric, err := metrics.NodeNetworkSendCount.AsFlatValue(sendDelta); err == nil {
			state.AddMetric(sendCountMetric)
		}

		// Calculate network per-second metrics
		timeDelta := currentTimestamp.Sub(cache.PreviousTimestamp).Seconds()
		if timeDelta > 0 {
			receivePerSecond := float64(receiveDelta) / timeDelta
			sendPerSecond := float64(sendDelta) / timeDelta

			if receiveRateMetric, err := metrics.NodeNetworkReceivePerSecond.AsFlatValue(receivePerSecond); err == nil {
				state.AddMetric(receiveRateMetric)
			}

			if sendRateMetric, err := metrics.NodeNetworkSendPerSecond.AsFlatValue(sendPerSecond); err == nil {
				state.AddMetric(sendRateMetric)
			}
		}

		// Update cache with current network values
		cache.PreviousNetworkReceiveBytes = networkStats.ReceiveBytesTotal
		cache.PreviousNetworkSendBytes = networkStats.TransmitBytesTotal
	} else if networkStats != nil {
		// First run - just store the values for next time
		cache.PreviousNetworkReceiveBytes = networkStats.ReceiveBytesTotal
		cache.PreviousNetworkSendBytes = networkStats.TransmitBytesTotal
	}

	// Collect disk usage (size is in system info, only usage percentage here)
	diskInfo, err := containerClient.DiskInfo(ctx)
	if err != nil {
		// Disk info might not be available
	} else {
		// Note: Disk size is now collected in PodSystemInfoCollector as it's static

		// Add disk usage percentage metric (dynamic)
		if diskUsageMetric, err := metrics.NodeDiskUsedPercentage.AsFlatValue(diskInfo.UsedPercent); err == nil {
			state.AddMetric(diskUsageMetric)
		}
	}

	// Collect load average
	loadAvg, err := containerClient.LoadAverage(ctx)
	if err != nil {
		// Load average might not be available
	} else {
		if loadAvgMetric, err := metrics.NodeLoadAverage.AsFlatValue(loadAvg); err == nil {
			state.AddMetric(loadAvgMetric)
		}
	}

	// Update cache with current CPU and memory values
	cache.PreviousCPUMillicores = cpuMillicores
	cache.PreviousMemoryBytes = memoryUsageBytes
	cache.PreviousTimestamp = currentTimestamp
	cache.Initialized = true

	// Store updated cache back to state
	state.Cache.HardwareCache["container_metrics"] = cache

	return nil
}
