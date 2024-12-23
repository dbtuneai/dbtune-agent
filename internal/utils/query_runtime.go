package utils

type CachedPGStatStatement struct {
	QueryId       string  `json:"query_id"`
	Calls         int     `json:"calls"`
	TotalExecTime float64 `json:"total_exec_time"`
}

// CalculateQueryRuntime calculates the runtime of
// a query based on two consecutive snapshots of the pg_stat_statements table
// and returns the runtime in milliseconds.
func CalculateQueryRuntime(prev, curr map[string]CachedPGStatStatement) float64 {
	totalExecTime := 0.0
	totalCalls := 0

	// Iterate over the current snapshot
	for queryId, currStat := range curr {
		// Get the previous stats, defaulting to zero if not found
		prevStat, exists := prev[queryId]
		if !exists {
			prevStat = CachedPGStatStatement{QueryId: queryId, Calls: 0, TotalExecTime: 0.0}
		}

		// Calculate the difference in calls and execution time
		queryIdCalls := currStat.Calls - prevStat.Calls
		queryIdExecTime := currStat.TotalExecTime - prevStat.TotalExecTime

		// Only consider queries that have had calls and execution time increase
		// There might be edge cases where the query has been reset, but we ignore those
		if queryIdCalls > 0 && queryIdExecTime > 0 {
			totalCalls += queryIdCalls
			totalExecTime += queryIdExecTime
		}
	}

	// Return the average execution time in milliseconds
	if totalCalls == 0 {
		return 0.0
	}
	return totalExecTime / float64(totalCalls)
}
