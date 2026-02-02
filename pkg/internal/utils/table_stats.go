package utils

import (
	"cmp"
	"maps"
	"slices"
	"time"
)

type PGUserTables struct {
	Name             string    `json:"name"`
	Oid              string    `json:"oid"`
	LastAutoVacuum   time.Time `json:"last_autovacuum"`
	LastAutoAnalyze  time.Time `json:"last_autoanalyze"`
	AutoVacuumCount  int64     `json:"autovacuum_count"`
	AutoAnalyzeCount int64     `json:"autoanalyze_count"`
	NLiveTup         int64     `json:"n_live_tup"`
	NDeadTup         int64     `json:"n_dead_tup"`
	NModSinceAnalyze int64     `json:"n_mod_since_analyze"`
	NInsSinceVacuum  int64     `json:"n_ins_since_vacuum"`
	SeqScan          int64     `json:"seq_scan"`
	SeqTupRead       int64     `json:"seq_tup_read"`
	IdxScan          int64     `json:"idx_scan"`
	IdxTupFetch      int64     `json:"idx_tup_fetch"`
}

// PGUserTableMetrics represents computed metrics for a single table (with deltas for cumulative fields)
type PGUserTableMetrics struct {
	LastAutoVacuum   *time.Time `json:"last_autovacuum,omitempty"`
	LastAutoAnalyze  *time.Time `json:"last_autoanalyze,omitempty"`
	AutoVacuumCount  int64      `json:"autovacuum_count"`
	AutoAnalyzeCount int64      `json:"autoanalyze_count"`
	NLiveTup         int64      `json:"n_live_tup"`
	NDeadTup         int64      `json:"n_dead_tup"`
	NModSinceAnalyze int64      `json:"n_mod_since_analyze"`
	NInsSinceVacuum  int64      `json:"n_ins_since_vacuum"`
	SeqScan          int64      `json:"seq_scan"`
	SeqTupRead       int64      `json:"seq_tup_read"`
	IdxScan          int64      `json:"idx_scan"`
	IdxTupFetch      int64      `json:"idx_tup_fetch"`
}

// FilterTopTables filters a map of table stats to keep only the top N tables
// sorted by total tuples (NLiveTup + NDeadTup) in descending order.
// If there are fewer tables than the limit, the original map is returned unchanged.
func FilterTopTables(tableStats map[string]PGUserTables, limit int) map[string]PGUserTables {
	if len(tableStats) <= limit {
		return tableStats
	}

	keys := slices.Collect(maps.Keys(tableStats))
	slices.SortFunc(keys, func(a, b string) int {
		totalA := tableStats[a].NLiveTup + tableStats[a].NDeadTup
		totalB := tableStats[b].NLiveTup + tableStats[b].NDeadTup
		return cmp.Or(
			cmp.Compare(totalB, totalA), // descending by total tuples
			cmp.Compare(a, b),           // ascending by key (tie-breaker)
		)
	})

	result := make(map[string]PGUserTables, limit)
	for _, k := range keys[:limit] {
		result[k] = tableStats[k]
	}
	return result
}
