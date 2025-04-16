package collectors

import (
	"context"
	"time"

	"github.com/aiven/go-client-codegen/handler/service"
	"github.com/dbtuneai/agent/pkg/adeptersinterfaces"
	"github.com/dbtuneai/agent/pkg/agent"
	aivenutil "github.com/dbtuneai/agent/pkg/aivenutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
)

// AivenHardwareInfo collects hardware metrics for Aiven PostgreSQL
func AivenHardwareInfo(adapter adeptersinterfaces.AivenPostgreSQLAdapter) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		aivenState := adapter.GetAivenState()
		aivenConfig := adapter.GetAivenConfig()
		if time.Since(aivenState.LastHardwareInfoTime) < aivenConfig.MetricResolutionSeconds {
			adapter.Logger().Debugf(
				"Hardware info already fetched in the last %s, skipping",
				aivenConfig.MetricResolutionSeconds,
			)
			return nil
		}
		aivenState.LastHardwareInfoTime = time.Now()
		fetchedMetrics, err := aivenutil.GetFetchedMetrics(ctx, aivenutil.FetchedMetricsIn{
			Client:      adapter.GetAivenClient(),
			ProjectName: adapter.GetAivenConfig().ProjectName,
			ServiceName: adapter.GetAivenConfig().ServiceName,
			Logger:      adapter.Logger(),
			Period:      service.PeriodTypeHour,
		})
		if err != nil {
			adapter.Logger().Errorf("Failed to get metrics: %v", err)
			return nil
		}

		// Process each metric type
		for _, maybeMetric := range fetchedMetrics {
			if maybeMetric.Error != nil {
				adapter.Logger().Debugf(
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
			state.AddMetric(metric)
		}

		// NOTE: Caching. As we need to re-fetch metrics for gaurdrails, and this API call
		// sends down a lot of data, we cache the results for the memory usage, which is
		// used by the gaurdrails.
		// The code will still work without this block.
		memAvailable := fetchedMetrics[aivenutil.MemAvailable]
		if memAvailable.Error != nil {
			// NOTE: We don't need to return an error for this, it's not a critical issue,
			// and the next call or Guardrails() will fetch the metrics again.
			return nil
		}

		aivenState.LastMemoryAvailablePercentage = memAvailable.ParsedMetric.Value.(float64)
		aivenState.LastMemoryAvailableTime = memAvailable.ParsedMetric.Timestamp
		return nil
	}
}
