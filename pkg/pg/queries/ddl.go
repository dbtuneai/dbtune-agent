package queries

// CollectDDL reconstructs CREATE TABLE and CREATE INDEX DDL statements from the
// PostgreSQL catalog for all tables in the 'public' schema.
//
// The DDL is built in a single query (no round trips) using CTEs that join
// pg_class, pg_attribute, pg_attrdef, pg_constraint, and pg_indexes.
//
// HashDDL computes a SHA-256 fingerprint of the DDL for change detection.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DDLName     = "ddl"
	DDLInterval = 1 * time.Minute
)

// HashDDL computes a SHA-256 hash of a DDL string for change detection.
func HashDDL(ddl string) string {
	hash := sha256.Sum256([]byte(ddl))
	return fmt.Sprintf("%x", hash)
}

const collectDDLQuery = `
WITH table_columns AS (
    SELECT
        c.relname AS table_name,
        string_agg(
            '    ' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
            || CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
            || CASE WHEN d.adbin IS NOT NULL THEN ' DEFAULT ' || pg_get_expr(d.adbin, d.adrelid) ELSE '' END,
            E',\n' ORDER BY a.attnum
        ) AS col_defs
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE n.nspname = 'public'
      AND c.relkind = 'r'
      AND a.attnum > 0
      AND NOT a.attisdropped
    GROUP BY c.relname
),
table_constraints AS (
    SELECT
        c.relname AS table_name,
        string_agg(
            '    ' || CASE con.contype WHEN 'p' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END
            || ' (' || (
                SELECT string_agg(att.attname, ', ' ORDER BY ord.n)
                FROM unnest(con.conkey) WITH ORDINALITY AS ord(attnum, n)
                JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ord.attnum
            ) || ')',
            E',\n' ORDER BY con.conname
        ) AS con_defs
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND con.contype IN ('p', 'u')
    GROUP BY c.relname
),
all_tables AS (
    SELECT
        tc.table_name,
        'CREATE TABLE ' || tc.table_name || E' (\n'
        || tc.col_defs
        || COALESCE(E',\n' || tcon.con_defs, '')
        || E'\n);\n' AS table_ddl
    FROM table_columns tc
    LEFT JOIN table_constraints tcon ON tcon.table_name = tc.table_name
),
all_indexes AS (
    SELECT string_agg(indexdef || ';', E'\n' ORDER BY indexname) AS idx_ddl
    FROM pg_indexes
    WHERE schemaname = 'public'
)
SELECT
    COALESCE(
        (SELECT string_agg(table_ddl, E'\n' ORDER BY table_name) FROM all_tables),
        ''
    )
    || E'\n'
    || COALESCE((SELECT idx_ddl FROM all_indexes), '');
`

type DDLPayload struct {
	DDL  string `json:"ddl"`
	Hash string `json:"hash"`
}

func DDLCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	tracker := newSkipTracker(skipUnchangedForceMultiplier)
	return CatalogCollector{
		Name:          DDLName,
		Interval:      DDLInterval,
		SkipUnchanged: true,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			var ddl string
			err = utils.QueryRowWithPrefix(pool, ctx, collectDDLQuery).Scan(&ddl)
			if err != nil {
				return nil, fmt.Errorf("failed to collect DDL: %w", err)
			}
			data, err := json.Marshal(&DDLPayload{DDL: ddl, Hash: HashDDL(ddl)})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", DDLName, err)
			}
			if tracker.shouldSkip(data) {
				return nil, nil
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
