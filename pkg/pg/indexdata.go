package pg

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/pg/catalog"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HashDDL computes a SHA-256 hash of a DDL string for change detection.
func HashDDL(ddl string) string {
	hash := sha256.Sum256([]byte(ddl))
	return fmt.Sprintf("%x", hash)
}

// ----------------------------------------------------------------------------
// SQL Queries
// ----------------------------------------------------------------------------

// schemaColumnsQuery returns all columns for all user tables in the public schema,
// including nullability and defaults needed to reconstruct valid CREATE TABLE DDL.
// NOTE: intentionally limited to the 'public' schema — this is the standard schema
// for application tables and matches the scope expected by the backend's index advisor.
const schemaColumnsQuery = `
SELECT
    c.relname AS table_name,
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    a.attnum AS column_position,
    a.attnotnull AS not_null,
    pg_get_expr(d.adbin, d.adrelid) AS column_default
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = 'public'
  AND c.relkind = 'r'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY c.relname, a.attnum;
`

// schemaConstraintsQuery returns primary key and unique constraints per table.
const schemaConstraintsQuery = `
SELECT
    c.relname AS table_name,
    con.contype::text AS constraint_type,
    array_agg(a.attname ORDER BY k.n) AS column_names
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, n)
JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
WHERE n.nspname = 'public'
  AND con.contype IN ('p', 'u')
GROUP BY c.relname, con.conname, con.contype
ORDER BY c.relname, con.conname;
`

// schemaIndexesQuery returns existing index definitions as full CREATE INDEX SQL.
const schemaIndexesQuery = `
SELECT indexdef
FROM pg_indexes
WHERE schemaname = 'public';
`

// ----------------------------------------------------------------------------
// DDL Collection
// ----------------------------------------------------------------------------

// tableColumn holds a single column's metadata from the catalog.
type tableColumn struct {
	tableName     string
	columnName    string
	dataType      string
	notNull       bool
	columnDefault *string
}

// tableConstraint holds a primary key or unique constraint.
type tableConstraint struct {
	tableName      string
	constraintType string // 'p' = primary key, 'u' = unique
	columnNames    []string
}

// CollectDDL queries the PostgreSQL catalog and reconstructs CREATE TABLE + CREATE INDEX DDL.
func CollectDDL(pgPool *pgxpool.Pool, ctx context.Context) (string, error) {
	ctx, cancel := catalog.EnsureTimeout(ctx)
	defer cancel()
	// 1. Collect columns
	columnRows, err := utils.QueryWithPrefix(pgPool, ctx, schemaColumnsQuery)
	if err != nil {
		return "", fmt.Errorf("failed to query schema columns: %w", err)
	}
	defer columnRows.Close()

	// Group columns by table, preserving order
	tableOrder := []string{}
	tableColumns := map[string][]tableColumn{}

	for columnRows.Next() {
		var col tableColumn
		var position int
		err := columnRows.Scan(
			&col.tableName, &col.columnName, &col.dataType,
			&position, &col.notNull, &col.columnDefault,
		)
		if err != nil {
			return "", fmt.Errorf("failed to scan column row: %w", err)
		}
		if _, exists := tableColumns[col.tableName]; !exists {
			tableOrder = append(tableOrder, col.tableName)
		}
		tableColumns[col.tableName] = append(tableColumns[col.tableName], col)
	}
	if err := columnRows.Err(); err != nil {
		return "", fmt.Errorf("error iterating column rows: %w", err)
	}

	// 2. Collect constraints
	constraintRows, err := utils.QueryWithPrefix(pgPool, ctx, schemaConstraintsQuery)
	if err != nil {
		return "", fmt.Errorf("failed to query schema constraints: %w", err)
	}
	defer constraintRows.Close()

	tableConstraints := map[string][]tableConstraint{}
	for constraintRows.Next() {
		var tc tableConstraint
		err := constraintRows.Scan(&tc.tableName, &tc.constraintType, &tc.columnNames)
		if err != nil {
			return "", fmt.Errorf("failed to scan constraint row: %w", err)
		}
		tableConstraints[tc.tableName] = append(tableConstraints[tc.tableName], tc)
	}
	if err := constraintRows.Err(); err != nil {
		return "", fmt.Errorf("error iterating constraint rows: %w", err)
	}

	// 3. Collect index definitions
	indexRows, err := utils.QueryWithPrefix(pgPool, ctx, schemaIndexesQuery)
	if err != nil {
		return "", fmt.Errorf("failed to query schema indexes: %w", err)
	}
	defer indexRows.Close()

	var indexDefs []string
	for indexRows.Next() {
		var indexDef string
		if err := indexRows.Scan(&indexDef); err != nil {
			return "", fmt.Errorf("failed to scan index row: %w", err)
		}
		indexDefs = append(indexDefs, indexDef)
	}
	if err := indexRows.Err(); err != nil {
		return "", fmt.Errorf("error iterating index rows: %w", err)
	}

	// 4. Build DDL string
	return buildDDL(tableOrder, tableColumns, tableConstraints, indexDefs), nil
}

func buildDDL(
	tableOrder []string,
	tableColumns map[string][]tableColumn,
	tableConstraints map[string][]tableConstraint,
	indexDefs []string,
) string {
	var b strings.Builder

	for _, tableName := range tableOrder {
		cols := tableColumns[tableName]
		b.WriteString("CREATE TABLE ")
		b.WriteString(tableName)
		b.WriteString(" (\n")

		for i, col := range cols {
			b.WriteString("    ")
			b.WriteString(col.columnName)
			b.WriteString(" ")
			b.WriteString(col.dataType)
			if col.notNull {
				b.WriteString(" NOT NULL")
			}
			if col.columnDefault != nil {
				b.WriteString(" DEFAULT ")
				b.WriteString(*col.columnDefault)
			}

			// Check if there are more columns or constraints to add
			hasMore := i < len(cols)-1 || len(tableConstraints[tableName]) > 0
			if hasMore {
				b.WriteString(",")
			}
			b.WriteString("\n")
		}

		// Append constraints
		constraints := tableConstraints[tableName]
		for i, tc := range constraints {
			b.WriteString("    ")
			if tc.constraintType == "p" {
				b.WriteString("PRIMARY KEY (")
			} else {
				b.WriteString("UNIQUE (")
			}
			b.WriteString(strings.Join(tc.columnNames, ", "))
			b.WriteString(")")
			if i < len(constraints)-1 {
				b.WriteString(",")
			}
			b.WriteString("\n")
		}

		b.WriteString(");\n\n")
	}

	// Append index definitions
	for _, indexDef := range indexDefs {
		b.WriteString(indexDef)
		b.WriteString(";\n")
	}

	return b.String()
}
