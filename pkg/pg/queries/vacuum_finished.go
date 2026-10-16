package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ALL-TABLES-VIEW

// VacuumFinishedRow holds the pg_stat_all_tables vacuum stats of a relation
// whose vacuum timestamps advanced. The tuple and page counts are read at
// collection time, not at the end of the vacuum.
type VacuumFinishedRow struct {
	RelID          *Oid         `json:"relid" db:"relid"`
	ParentRelID    *Oid         `json:"parent_relid" db:"parent_relid"`
	SchemaName     *Name        `json:"schemaname" db:"schemaname"`
	RelName        *Name        `json:"relname" db:"relname"`
	LastVacuum     *TimestampTZ `json:"last_vacuum" db:"last_vacuum"`
	LastAutovacuum *TimestampTZ `json:"last_autovacuum" db:"last_autovacuum"`
	NDeadTup       *Bigint      `json:"n_dead_tup" db:"n_dead_tup"`
	NLiveTup       *Bigint      `json:"n_live_tup" db:"n_live_tup"`
	RelPages       *Integer     `json:"relpages" db:"relpages"`
	RelAllVisible  *Integer     `json:"relallvisible" db:"relallvisible"`
	RelAllFrozen   *Integer     `json:"relallfrozen" db:"relallfrozen"`
}

// TOAST tables are vacuumed as relations of their own; pt is the owning table.
// reltoastrelid is not indexed, so this is a join (one hashed scan of pg_class)
// rather than a correlated subquery, which would scan pg_class once per TOAST
// row. pg_depend can't replace it: catalogs' TOAST tables are pinned and have
// no entries there.
const vacuumFinishedParentJoin = `LEFT JOIN pg_class pt ON c.relkind = 't' AND pt.reltoastrelid = c.oid`

// The query reads pg_class and the pg_stat_get_* functions directly rather
// than pg_stat_all_tables: that view joins pg_index and groups every relation
// to sum index scans (~60 ms of extra work per tick at 40k relations). Only the
// two timestamp lookups run per relation; the remaining stats are fetched for
// matching rows only. Partitioned tables are left out because vacuum stats are
// only reported for their leaf partitions.
//
// %s: the relallfrozen column (PG18+), then the parent join.
const vacuumFinishedQuery = `SELECT
	c.oid AS relid,
	pt.oid AS parent_relid,
	n.nspname AS schemaname,
	c.relname,
	pg_stat_get_last_vacuum_time(c.oid) AS last_vacuum,
	pg_stat_get_last_autovacuum_time(c.oid) AS last_autovacuum,
	pg_stat_get_dead_tuples(c.oid) AS n_dead_tup,
	pg_stat_get_live_tuples(c.oid) AS n_live_tup,
	c.relpages,
	c.relallvisible,
	%s
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
%s
WHERE c.relkind IN ('r', 't', 'm')
	AND (pg_stat_get_last_vacuum_time(c.oid) >= $1
		OR pg_stat_get_last_autovacuum_time(c.oid) >= $1)`

func vacuumFinishedColumns(pgMajorVersion int) string {
	if pgMajorVersion >= 18 {
		return `c.relallfrozen`
	}
	return `NULL::integer AS relallfrozen`
}
