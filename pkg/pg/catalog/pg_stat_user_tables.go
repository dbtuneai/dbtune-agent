package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatUserTablesName     = "pg_stat_user_tables"
	PgStatUserTablesInterval = 1 * time.Minute
)

const pgStatUserTablesQuery = `SELECT * FROM pg_stat_user_tables ORDER BY schemaname, relname`

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
	RelID                *int64   `json:"relid" db:"relid"`
	SchemaName           string   `json:"schemaname" db:"schemaname"`
	RelName              string   `json:"relname" db:"relname"`
	SeqScan              *int64   `json:"seq_scan" db:"seq_scan"`
	LastSeqScan          *string  `json:"last_seq_scan" db:"last_seq_scan"`
	SeqTupRead           *int64   `json:"seq_tup_read" db:"seq_tup_read"`
	IdxScan              *int64   `json:"idx_scan" db:"idx_scan"`
	LastIdxScan          *string  `json:"last_idx_scan" db:"last_idx_scan"`
	IdxTupFetch          *int64   `json:"idx_tup_fetch" db:"idx_tup_fetch"`
	NTupIns              *int64   `json:"n_tup_ins" db:"n_tup_ins"`
	NTupUpd              *int64   `json:"n_tup_upd" db:"n_tup_upd"`
	NTupDel              *int64   `json:"n_tup_del" db:"n_tup_del"`
	NTupHotUpd           *int64   `json:"n_tup_hot_upd" db:"n_tup_hot_upd"`
	NTupNewpageUpd       *int64   `json:"n_tup_newpage_upd" db:"n_tup_newpage_upd"`
	NLiveTup             *int64   `json:"n_live_tup" db:"n_live_tup"`
	NDeadTup             *int64   `json:"n_dead_tup" db:"n_dead_tup"`
	NModSinceAnalyze     *int64   `json:"n_mod_since_analyze" db:"n_mod_since_analyze"`
	NInsSinceVacuum      *int64   `json:"n_ins_since_vacuum" db:"n_ins_since_vacuum"`
	LastVacuum           *string  `json:"last_vacuum" db:"last_vacuum"`
	LastAutovacuum       *string  `json:"last_autovacuum" db:"last_autovacuum"`
	LastAnalyze          *string  `json:"last_analyze" db:"last_analyze"`
	LastAutoanalyze      *string  `json:"last_autoanalyze" db:"last_autoanalyze"`
	VacuumCount          *int64   `json:"vacuum_count" db:"vacuum_count"`
	AutovacuumCount      *int64   `json:"autovacuum_count" db:"autovacuum_count"`
	AnalyzeCount         *int64   `json:"analyze_count" db:"analyze_count"`
	AutoanalyzeCount     *int64   `json:"autoanalyze_count" db:"autoanalyze_count"`
	TotalVacuumTime      *float64 `json:"total_vacuum_time" db:"total_vacuum_time"`
	TotalAutovacuumTime  *float64 `json:"total_autovacuum_time" db:"total_autovacuum_time"`
	TotalAnalyzeTime     *float64 `json:"total_analyze_time" db:"total_analyze_time"`
	TotalAutoanalyzeTime *float64 `json:"total_autoanalyze_time" db:"total_autoanalyze_time"`
}

// PgStatUserTablePayload is the JSON body POSTed to /api/v1/agent/pg_stat_user_tables.
type PgStatUserTablePayload struct {
	Rows []PgStatUserTableRow `json:"rows"`
}
