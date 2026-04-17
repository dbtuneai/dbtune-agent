package queries

// https://www.postgresql.org/docs/current/view-pg-settings.html

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgSettingsRow represents a single row from the pg_settings view.
// All columns are included; nullable columns use pointer types.
// The Scanner gracefully handles missing or extra columns across PG versions
// (unknown columns are skipped, missing columns get zero values).
type PgSettingsRow struct {
	Name           Text     `json:"name" db:"name"`
	Setting        Text     `json:"setting" db:"setting"`
	Unit           *Text    `json:"unit" db:"unit"`
	Category       Text     `json:"category" db:"category"`
	ShortDesc      Text     `json:"short_desc" db:"short_desc"`
	ExtraDesc      *Text    `json:"extra_desc" db:"extra_desc"`
	Context        Text     `json:"context" db:"context"`
	Vartype        Text     `json:"vartype" db:"vartype"`
	Source         Text     `json:"source" db:"source"`
	MinVal         *Text    `json:"min_val" db:"min_val"`
	MaxVal         *Text    `json:"max_val" db:"max_val"`
	EnumVals       []Text   `json:"enumvals" db:"enumvals"`
	BootVal        *Text    `json:"boot_val" db:"boot_val"`
	ResetVal       *Text    `json:"reset_val" db:"reset_val"`
	Sourcefile     *Text    `json:"sourcefile" db:"sourcefile"`
	Sourceline     *Integer `json:"sourceline" db:"sourceline"`
	PendingRestart Boolean  `json:"pending_restart" db:"pending_restart"`
}

const pgSettingsQuery = `SELECT * FROM pg_settings`

var pgSettingsScanner = pgxutil.NewScanner[PgSettingsRow]()

// QueryPgSettings returns all rows from the pg_settings view.
// This is a shared utility used by the tuning loop (GetActiveConfig) and
// can later back a catalog collector.
func QueryPgSettings(pool *pgxpool.Pool, ctx context.Context) ([]PgSettingsRow, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, pgSettingsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_settings: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgSettingsScanner.Scan)
}
