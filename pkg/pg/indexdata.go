package pg

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/pg/catalog"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HashDDL computes a SHA-256 hash of a DDL string for change detection.
func HashDDL(ddl string) string {
	hash := sha256.Sum256([]byte(ddl))
	return fmt.Sprintf("%x", hash)
}

// collectDDLQuery builds the full CREATE TABLE + CREATE INDEX DDL in a single
// query, avoiding multiple round trips and ensuring a consistent catalog snapshot.
//
// NOTE: intentionally limited to the 'public' schema — this is the standard schema
// for application tables and matches the scope expected by the backend's index advisor.
const collectDDLQuery = `
WITH table_columns AS (
    SELECT
        c.relname AS table_name,
        string_agg(
            '    ' || a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod)
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

// CollectDDL queries the PostgreSQL catalog and reconstructs CREATE TABLE + CREATE INDEX DDL.
func CollectDDL(pgPool *pgxpool.Pool, ctx context.Context) (string, error) {
	ctx, cancel := catalog.EnsureTimeout(ctx)
	defer cancel()

	var ddl string
	err := utils.QueryRowWithPrefix(pgPool, ctx, collectDDLQuery).Scan(&ddl)
	if err != nil {
		return "", fmt.Errorf("failed to collect DDL: %w", err)
	}

	return ddl, nil
}
