package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ALL-TABLES

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatUserTablesName     = "pg_stat_user_tables"
	PgStatUserTablesInterval = 1 * time.Minute

	// PgStatUserTablesCategoryLimit caps each activity category in the UNION
	// query. The final result contains at most 3× this value (before dedup),
	// but in practice heavy-write tables are often heavy-read too, so the
	// actual count is usually well below the theoretical max.
	PgStatUserTablesCategoryLimit = 200
)

// pgStatUserTablesQuery samples the most interesting tables from three
// complementary perspectives, ensuring no single activity dimension
// drowns out the others:
//
//  1. Write activity (n_tup_ins + n_tup_upd + n_tup_del) — tables with the
//     most churn are the primary tuning targets for autovacuum, fillfactor,
//     and index maintenance.
//
//  2. Read activity (seq_scan + idx_scan) — tables scanned most often,
//     including read-only/static tables that may benefit from missing-index
//     analysis. Without this category, a large static table hammered by
//     sequential scans would never surface.
//
//  3. Dead-tuple pressure (n_dead_tup) — tables accumulating bloat that
//     may need vacuum tuning, regardless of current read/write rate.
//
// UNION deduplicates across categories automatically.
var pgStatUserTablesQuery = fmt.Sprintf(`
(SELECT * FROM pg_stat_user_tables ORDER BY COALESCE(n_tup_ins,0) + COALESCE(n_tup_upd,0) + COALESCE(n_tup_del,0) DESC LIMIT %d)
UNION
(SELECT * FROM pg_stat_user_tables ORDER BY COALESCE(seq_scan,0) + COALESCE(idx_scan,0) DESC LIMIT %d)
UNION
(SELECT * FROM pg_stat_user_tables ORDER BY COALESCE(n_dead_tup,0) DESC LIMIT %d)
`, PgStatUserTablesCategoryLimit, PgStatUserTablesCategoryLimit, PgStatUserTablesCategoryLimit)

func CollectPgStatUserTables(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatUserTableRow, error) {
	return CollectView[PgStatUserTableRow](pgPool, ctx, pgStatUserTablesQuery, "pg_stat_user_tables")
}

func NewPgStatUserTablesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatUserTablesName,
		Interval: PgStatUserTablesInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatUserTables(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatUserTablePayload{Rows: rows}, nil
		},
	}
}

// PgStatUserTableRow represents a single row from pg_stat_user_tables,
// matching the backend's PgStatUserTable Django model.
type PgStatUserTableRow struct {
	RelID                *Oid              `json:"relid" db:"relid"`
	SchemaName           Name              `json:"schemaname" db:"schemaname"`
	RelName              Name              `json:"relname" db:"relname"`
	SeqScan              *Bigint           `json:"seq_scan" db:"seq_scan"`
	LastSeqScan          *TimestampTZ      `json:"last_seq_scan" db:"last_seq_scan"`
	SeqTupRead           *Bigint           `json:"seq_tup_read" db:"seq_tup_read"`
	IdxScan              *Bigint           `json:"idx_scan" db:"idx_scan"`
	LastIdxScan          *TimestampTZ      `json:"last_idx_scan" db:"last_idx_scan"`
	IdxTupFetch          *Bigint           `json:"idx_tup_fetch" db:"idx_tup_fetch"`
	NTupIns              *Bigint           `json:"n_tup_ins" db:"n_tup_ins"`
	NTupUpd              *Bigint           `json:"n_tup_upd" db:"n_tup_upd"`
	NTupDel              *Bigint           `json:"n_tup_del" db:"n_tup_del"`
	NTupHotUpd           *Bigint           `json:"n_tup_hot_upd" db:"n_tup_hot_upd"`
	NTupNewpageUpd       *Bigint           `json:"n_tup_newpage_upd" db:"n_tup_newpage_upd"`
	NLiveTup             *Bigint           `json:"n_live_tup" db:"n_live_tup"`
	NDeadTup             *Bigint           `json:"n_dead_tup" db:"n_dead_tup"`
	NModSinceAnalyze     *Bigint           `json:"n_mod_since_analyze" db:"n_mod_since_analyze"`
	NInsSinceVacuum      *Bigint           `json:"n_ins_since_vacuum" db:"n_ins_since_vacuum"`
	LastVacuum           *TimestampTZ      `json:"last_vacuum" db:"last_vacuum"`
	LastAutovacuum       *TimestampTZ      `json:"last_autovacuum" db:"last_autovacuum"`
	LastAnalyze          *TimestampTZ      `json:"last_analyze" db:"last_analyze"`
	LastAutoanalyze      *TimestampTZ      `json:"last_autoanalyze" db:"last_autoanalyze"`
	VacuumCount          *Bigint           `json:"vacuum_count" db:"vacuum_count"`
	AutovacuumCount      *Bigint           `json:"autovacuum_count" db:"autovacuum_count"`
	AnalyzeCount         *Bigint           `json:"analyze_count" db:"analyze_count"`
	AutoanalyzeCount     *Bigint           `json:"autoanalyze_count" db:"autoanalyze_count"`
	TotalVacuumTime      *DoublePrecision  `json:"total_vacuum_time" db:"total_vacuum_time"`
	TotalAutovacuumTime  *DoublePrecision  `json:"total_autovacuum_time" db:"total_autovacuum_time"`
	TotalAnalyzeTime     *DoublePrecision  `json:"total_analyze_time" db:"total_analyze_time"`
	TotalAutoanalyzeTime *DoublePrecision  `json:"total_autoanalyze_time" db:"total_autoanalyze_time"`
}

// PgStatUserTablePayload is the JSON body POSTed to /api/v1/agent/pg_stat_user_tables.
type PgStatUserTablePayload struct {
	Rows []PgStatUserTableRow `json:"rows"`
}
