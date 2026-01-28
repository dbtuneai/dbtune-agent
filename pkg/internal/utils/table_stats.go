package utils

import (
	"sort"
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

	type tableEntry struct {
		key   string
		stats PGUserTables
	}
	entries := make([]tableEntry, 0, len(tableStats))
	for k, v := range tableStats {
		entries = append(entries, tableEntry{key: k, stats: v})
	}
	sort.Slice(entries, func(i, j int) bool {
		totalI := entries[i].stats.NLiveTup + entries[i].stats.NDeadTup
		totalJ := entries[j].stats.NLiveTup + entries[j].stats.NDeadTup
		if totalI != totalJ {
			return totalI > totalJ
		}
		return entries[i].key < entries[j].key
	})

	result := make(map[string]PGUserTables, limit)
	for i := range limit {
		result[entries[i].key] = entries[i].stats
	}
	return result
}
