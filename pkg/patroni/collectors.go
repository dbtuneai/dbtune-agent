package patroni

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
)

func HardwareInfoPatroni() func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		cpuPercentage, _ := cpu.Percent(time.Millisecond*100, false) // Report the average CPU usage over 100ms
		cpuModelMetric, _ := metrics.NodeCPUUsage.AsFlatValue(cpuPercentage[0])
		state.AddMetric(cpuModelMetric)

		// Get Reads and Write IOps
		ioCounters, _ := disk.IOCounters()
		// Get the total Read and Write IOps
		// Update the cache state, and on the next iteration,
		// calculate the difference
		var writes, reads uint64

		for _, ioCounter := range ioCounters {
			writes += ioCounter.WriteCount
			reads += ioCounter.ReadCount
		}

		if state.Cache.IOCountersStat != (agent.IOCounterStat{}) {
			totalIOps := (reads + writes) - (state.Cache.IOCountersStat.ReadCount + state.Cache.IOCountersStat.WriteCount)
			iopsTotalMetric, _ := metrics.NodeDiskIOTotalCount.AsFlatValue(totalIOps)
			state.AddMetric(iopsTotalMetric)

			readCountMetric, _ := metrics.NodeDiskIOReadCount.AsFlatValue(reads - state.Cache.IOCountersStat.ReadCount)
			state.AddMetric(readCountMetric)

			writeCountMetric, _ := metrics.NodeDiskIOWriteCount.AsFlatValue(writes - state.Cache.IOCountersStat.WriteCount)
			state.AddMetric(writeCountMetric)
		}

		// Update cache
		state.Cache.IOCountersStat = agent.IOCounterStat{ReadCount: reads, WriteCount: writes}

		// Memory usage
		memoryInfo, _ := mem.VirtualMemory()
		usedMemory, _ := metrics.NodeMemoryUsed.AsFlatValue(memoryInfo.Total - memoryInfo.Available)
		state.AddMetric(usedMemory)

		return nil
	}
}
