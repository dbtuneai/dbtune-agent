package aiven

import (
	"context"
	"time"

	aivenclient "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/service"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/sirupsen/logrus"
)

// AivenHardwareInfo collects hardware metrics for Aiven PostgreSQL
func AivenHardwareInfo(
	client *aivenclient.Client,
	projectName string,
	serviceName string,
	metricResolution time.Duration,
	config Config,
	State *State,
	logger *logrus.Logger,
) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, metricState *agent.MetricsState) error {
		if time.Since(State.Hardware.LastChecked) < metricResolution {
			logger.Debugf(
				"Hardware info already fetched in the last %s, skipping",
				metricResolution,
			)
			return nil
		}
		State.LastHardwareInfoTime = time.Now()
		fetchedMetrics, err := GetFetchedMetrics(ctx, FetchedMetricsIn{
			Client:      client,
			ProjectName: projectName,
			ServiceName: serviceName,
			Logger:      logger,
			Period:      service.PeriodTypeHour,
		})
		if err != nil {
			logger.Errorf("Failed to get metrics: %v", err)
			return nil
		}

		// Process each metric type
		for _, maybeMetric := range fetchedMetrics {
			if maybeMetric.Error != nil {
				logger.Debugf(
					"Failed to get metric %s: %v",
					maybeMetric.ParsedMetric.Name,
					maybeMetric.Error,
				)
				continue
			}

			value, _ := maybeMetric.ParsedMetric.AsFlatValue()
			metricState.AddMetric(value)
		}

		// Calculate the total IOPS from the read and write IOPS if exist
		var readIOPSValue *float64
		readIOPS, okRead := fetchedMetrics[DISK_IO_READ_KEY]
		if okRead && readIOPS.Value != nil && readIOPS.Error == nil {
			v, ok := readIOPS.Value.(float64)
			if ok {
				readIOPSValue = &v
			}
		}
		var writeIOPSValue *float64
		writeIOPS, okWrite := fetchedMetrics[DISK_IO_WRITES_KEY]
		if okWrite && writeIOPS.Value != nil && writeIOPS.Error == nil {
			v, ok := writeIOPS.Value.(float64)
			if ok {
				writeIOPSValue = &v
			}
		}

		if readIOPSValue != nil && writeIOPSValue != nil {
			totalIOPS := *readIOPSValue + *writeIOPSValue
			totalIOPSMetric, _ := metrics.NodeDiskIOPSTotalPerSecond.AsFlatValue(totalIOPS)
			metricState.AddMetric(totalIOPSMetric)
		}

		// NOTE: Caching. As we need to re-fetch metrics for guardrails, and this API call
		// sends down a lot of data, we cache the results for the memory usage, which is
		// used by the guardrails.
		// The code will still work without this block.
		memAvailable, okMem := fetchedMetrics[MEM_AVAILABLE_KEY]
		if okMem && memAvailable.Value != nil && memAvailable.Error == nil {
			// NOTE: We don't need to return an error for this, it's not a critical issue,
			// and the next call or Guardrails() will fetch the metrics again.
			State.LastMemoryAvailablePercentage = memAvailable.ParsedMetric.Value.(float64)
			State.LastMemoryAvailableTime = memAvailable.ParsedMetric.Timestamp
		} else {
			logger.Warnf("Failed to get memory available metric: %v", memAvailable.Error)
		}

		return nil
	}
}
