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

// AivenQueryRuntime is a specialized version of the query runtime collector for Aiven PostgreSQL
// It handles the specific limitations and behaviors of Aiven's PostgreSQL implementation
func AivenQueryRuntime(pgAdapter adeptersinterfaces.PostgreSQLAdapter) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		// Check if pg_stat_statements extension is available
		var extensionExists bool
		checkExtQuery := `
		/*dbtune*/
		SELECT EXISTS (
			SELECT 1 
			FROM pg_extension 
			WHERE extname = 'pg_stat_statements'
		);
		`

		err := pgAdapter.PGDriver().QueryRow(ctx, checkExtQuery).Scan(&extensionExists)
		if err != nil {
			pgAdapter.Logger().Warnf("Failed to check for pg_stat_statements extension: %v", err)
			return nil // Continue with other collectors
		}

		if !extensionExists {
			pgAdapter.Logger().Error("pg_stat_statements extension is not installed, skipping query runtime collection. Please install the extension to collect query statistics.")
			return nil // Continue with other collectors
		}

		// Get the query statistics in a format that works reliably with Aiven
		query := `
		/*dbtune*/
		SELECT 
			queryid::text as query_id,
			calls,
			total_exec_time
		FROM pg_stat_statements 
		WHERE query NOT LIKE '%dbtune%'
		AND queryid IS NOT NULL
		ORDER BY total_exec_time DESC
		LIMIT 100;
		`

		rows, err := pgAdapter.PGDriver().Query(ctx, query)
		if err != nil {
			pgAdapter.Logger().Warnf("Error querying pg_stat_statements: %v", err)
			return nil // Continue with other collectors
		}
		defer rows.Close()

		// Build a map of query stats similar to what would be constructed from JSON
		queryStats := make(map[string]utils.CachedPGStatStatement)
		for rows.Next() {
			var queryID string
			var calls int64
			var totalExecTime float64

			err := rows.Scan(&queryID, &calls, &totalExecTime)
			if err != nil {
				pgAdapter.Logger().Warnf("Error scanning query stat row: %v", err)
				continue
			}

			queryStats[queryID] = utils.CachedPGStatStatement{
				Calls:         int(calls),
				TotalExecTime: totalExecTime,
			}
		}

		// Process the results using the same approach as the standard collector
		if len(queryStats) == 0 {
			pgAdapter.Logger().Info("No query statistics available")
			return nil
		}

		if state.Cache.QueryRuntimeList == nil {
			pgAdapter.Logger().Info("Cache miss, filling data")
			state.Cache.QueryRuntimeList = queryStats
		} else {
			// Calculate the runtime using the same utility function as the standard collector
			runtime := utils.CalculateQueryRuntime(state.Cache.QueryRuntimeList, queryStats)

			metricEntry, err := utils.NewMetric("perf_average_query_runtime", runtime, utils.Float)
			if err != nil {
				return err
			}

			state.Cache.QueryRuntimeList = queryStats
			state.AddMetric(metricEntry)
		}

		return nil
	}
}

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
