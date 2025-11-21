package utils

import "time"

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

type PGClass struct {
	UnfrozenAge     int64 `json:"unfrozen_age"`
	UnfrozenMXIDAge int64 `json:"unfrozen_mxid_age"`
	RelAllVisible   int64 `json:"relallvisible"`
}

type PGStatProgressVacuum struct {
	Phase            int64 `json:"phase"`
	HeapBlksTotal    int64 `json:"heap_blks_total"`
	HeapBlksScanned  int64 `json:"heap_blks_scanned"`
	HeapBlksVacuumed int64 `json:"heap_blks_vacuumed"`
	IndexVacuumCount int64 `json:"index_vacuum_count"`
}
