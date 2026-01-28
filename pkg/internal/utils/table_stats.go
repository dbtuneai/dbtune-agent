package utils

import "time"

type PGUserTables struct {
	Name             string
	Relid            uint32
	LastAutoVacuum   time.Time
	LastAutoAnalyze  time.Time
	AutoVacuumCount  int64
	AutoAnalyzeCount int64
	NLiveTup         int64
	NDeadTup         int64
	NModSinceAnalyze int64
	NInsSinceVacuum  int64
	SeqScan          int64
	SeqTupRead       int64
	IdxScan          int64
	IdxTupFetch      int64
}

type PGClass struct {
	UnfrozenAge     int64
	UnfrozenMXIDAge int64
	RelAllVisible   int64
}

type PGStatProgressVacuum struct {
	Phase            int64
	HeapBlksTotal    int64
	HeapBlksScanned  int64
	HeapBlksVacuumed int64
	IndexVacuumCount int64
}
