package collectors

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/adeptersinterfaces"
	"github.com/dbtuneai/agent/pkg/agent"
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
			pgAdapter.Logger().Warn("pg_stat_statements extension is not installed, skipping query runtime collection")
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
				Calls:         calls,
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
