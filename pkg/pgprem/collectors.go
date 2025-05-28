package pgprem

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
)

func HardwareInfoOnPremise() func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		cpuPercentage, _ := cpu.Percent(time.Millisecond*100, false) // Report the average CPU usage over 100ms
		cpuModelMetric, _ := utils.NewMetric("node_cpu_usage", cpuPercentage[0], utils.Float)
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
			iopsTotalMetric, _ := utils.NewMetric("node_disk_io_ops_total", totalIOps, utils.Int)
			state.AddMetric(iopsTotalMetric)

			readCountMetric, _ := utils.NewMetric("node_disk_io_ops_read", reads-state.Cache.IOCountersStat.ReadCount, utils.Int)
			state.AddMetric(readCountMetric)

			writeCountMetric, _ := utils.NewMetric("node_disk_io_ops_write", writes-state.Cache.IOCountersStat.WriteCount, utils.Int)
			state.AddMetric(writeCountMetric)
		}

		// Update cache
		state.Cache.IOCountersStat = agent.IOCounterStat{ReadCount: reads, WriteCount: writes}

		// Memory usage
		memoryInfo, _ := mem.VirtualMemory()
		usedMemory, _ := utils.NewMetric("node_memory_used", memoryInfo.Total-memoryInfo.Available, utils.Int)
		state.AddMetric(usedMemory)

		return nil
	}
}
