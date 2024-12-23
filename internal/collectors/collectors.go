package collectors

import (
	"context"
	"encoding/json"
	"example.com/dbtune-agent/internal/utils"
	"fmt"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
	"regexp"
)

func QueryRuntime(driver *pgPool.Pool) func(state *utils.MetricsState) error {
	// Perform runtime reflection to make sure that the
	// struct is embedding the default adapter
	return func(state *utils.MetricsState) error {

		var query = `
		SELECT JSON_OBJECT_AGG(queryid, JSON_BUILD_OBJECT('calls',calls,'total_exec_time',total_exec_time))
        AS qrt_stats
        FROM (SELECT * FROM pg_stat_statements WHERE query NOT LIKE '%dbtune%' ORDER BY calls DESC)
        AS f
		`

		var jsonResult string
		err := driver.QueryRow(context.Background(), query).Scan(&jsonResult)
		if err != nil {
			return err
		}

		var queryStats map[string]utils.CachedPGStatStatement
		err = json.Unmarshal([]byte(jsonResult), &queryStats)
		if err != nil {
			return err
		}

		if state.Cache.QueryRuntimeList == nil {
			fmt.Println("Cache miss, filling data")
			state.Cache.QueryRuntimeList = queryStats
		} else {
			runtime := utils.CalculateQueryRuntime(state.Cache.QueryRuntimeList, queryStats)

			metricEntry, err := utils.NewMetric("database_avg_query_runtime", runtime, utils.Float)
			if err != nil {
				return err
			}

			state.Cache.QueryRuntimeList = queryStats
			state.Metrics = append(state.Metrics, metricEntry)
		}

		return nil
	}
}

func ActiveConnections(driver *pgPool.Pool) func(state *utils.MetricsState) error {
	// Perform runtime reflection to make sure that the
	// struct is embedding the default adapter
	return func(state *utils.MetricsState) error {

		var query = `
		/*dbtune*/
		SELECT COUNT(*) AS active_connections
		FROM pg_stat_activity
		WHERE state = 'active'
		`

		var result int
		err := driver.QueryRow(context.Background(), query).Scan(&result)
		if err != nil {
			return err
		}

		metricEntry, err := utils.NewMetric("database_active_connections", result, utils.Int)
		if err != nil {
			return err
		}
		state.Metrics = append(state.Metrics, metricEntry)

		return nil
	}
}

func TransactionsPerSecond(driver *pgPool.Pool) func(state *utils.MetricsState) error {
	return func(state *utils.MetricsState) error {

		var query = `
		/*dbtune*/
	    SELECT SUM(xact_commit)::bigint
		AS server_xact_commits
		FROM pg_stat_database;
		`

		var serverXactCommits int64
		err := driver.QueryRow(context.Background(), query).Scan(&serverXactCommits)
		if err != nil {
			return err
		}

		fmt.Println("Here")
		fmt.Println(state.Cache.XactCommit)
		if state.Cache.XactCommit == 0 {
			fmt.Println("Cache miss, filling data")
			state.Cache.XactCommit = serverXactCommits
			return nil
		}

		metricEntry, err := utils.NewMetric("database_tps", serverXactCommits-state.Cache.XactCommit, utils.Int)
		if err != nil {
			return err
		}
		state.Metrics = append(state.Metrics, metricEntry)

		// Update caches
		state.Cache.XactCommit = serverXactCommits

		return nil
	}
}

// PGVersion returns the version of the PostgreSQL instance
// Example: 16.4
func PGVersion(driver *pgPool.Pool) (string, error) {
	var pgVersion string
	versionRegex := regexp.MustCompile(`PostgreSQL (\d+\.\d+)`)
	err := driver.QueryRow(context.Background(), "SELECT version();").Scan(&pgVersion)
	if err != nil {
		return "", err
	}
	matches := versionRegex.FindStringSubmatch(pgVersion)

	return matches[1], nil
}
