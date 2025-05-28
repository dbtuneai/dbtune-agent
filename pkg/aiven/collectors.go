package aiven

import (
	"context"
	"time"

	aivenclient "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/service"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
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

			parsedMetric := maybeMetric.ParsedMetric
			metric, _ := utils.NewMetric(
				parsedMetric.RenameTo,
				parsedMetric.Value,
				parsedMetric.Type,
			)
			metricState.AddMetric(metric)
		}

		// NOTE: Caching. As we need to re-fetch metrics for gaurdrails, and this API call
		// sends down a lot of data, we cache the results for the memory usage, which is
		// used by the gaurdrails.
		// The code will still work without this block.
		memAvailable := fetchedMetrics[MEM_AVAILABLE_KEY]
		if memAvailable.Error != nil {
			// NOTE: We don't need to return an error for this, it's not a critical issue,
			// and the next call or Guardrails() will fetch the metrics again.
			return nil
		}

		State.LastMemoryAvailablePercentage = memAvailable.ParsedMetric.Value.(float64)
		State.LastMemoryAvailableTime = memAvailable.ParsedMetric.Timestamp
		return nil
	}
}
