package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

const schemaHashQuery = `
SELECT md5(string_agg(
    c.relname || ':' || a.attname || ':' || t.typname || ':' || coalesce(i.indexdef, ''),
    ',' ORDER BY c.relname, a.attname
))
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_indexes i ON i.tablename = c.relname AND i.schemaname = n.nspname
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND c.relkind IN ('r', 'p')
  AND a.attnum > 0
  AND NOT a.attisdropped;
`

const schemaCountsQuery = `
SELECT
    (SELECT count(*) FROM pg_catalog.pg_class c
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
       AND c.relkind IN ('r', 'p')) AS table_count,
    (SELECT count(*) FROM pg_catalog.pg_class c
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
       AND c.relkind = 'i') AS index_count,
    (SELECT count(*) FROM pg_catalog.pg_constraint con
     JOIN pg_catalog.pg_namespace n ON n.oid = con.connamespace
     WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')) AS constraint_count;
`

const schemaTablesQuery = `
SELECT c.oid::bigint AS table_id,
       c.relname AS table_name,
       a.attname AS column_name,
       t.typname AS data_type
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND c.relkind IN ('r', 'p')
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY c.relname, a.attnum
LIMIT $1;
`

const schemaIndexesQuery = `
SELECT c.oid::bigint AS index_id,
       pg_catalog.pg_get_indexdef(c.oid) AS index_definition
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND c.relkind = 'i'
ORDER BY c.relname
LIMIT $1;
`

// CollectSchemaSnapshot gathers the database schema snapshot.
// It always collects the schema hash and counts.
// If allowDDLCollection is true, it also collects table columns and index definitions.
func CollectSchemaSnapshot(pool *pgxpool.Pool, allowDDLCollection bool, logger *logrus.Logger) (*agent.SchemaSnapshot, error) {
	ctx := context.Background()
	snapshot := &agent.SchemaSnapshot{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	// Always collect hash
	var schemaHash *string
	err := utils.QueryRowWithPrefix(pool, ctx, schemaHashQuery).Scan(&schemaHash)
	if err != nil {
		return nil, fmt.Errorf("schema hash query failed: %w", err)
	}
	if schemaHash != nil {
		snapshot.SchemaHash = *schemaHash
	}

	// Always collect counts
	err = utils.QueryRowWithPrefix(pool, ctx, schemaCountsQuery).Scan(
		&snapshot.TableCount,
		&snapshot.IndexCount,
		&snapshot.ConstraintCount,
	)
	if err != nil {
		return nil, fmt.Errorf("schema counts query failed: %w", err)
	}

	// Conditionally collect DDL details
	if allowDDLCollection {
		// Collect table columns
		rows, err := utils.QueryWithPrefix(pool, ctx, schemaTablesQuery, agent.MaxDDLTableColumns)
		if err != nil {
			return nil, fmt.Errorf("schema tables query failed: %w", err)
		}
		defer rows.Close()

		var tables []agent.SchemaColumn
		for rows.Next() {
			var col agent.SchemaColumn
			if err := rows.Scan(&col.TableID, &col.TableName, &col.ColumnName, &col.DataType); err != nil {
				return nil, fmt.Errorf("schema tables scan failed: %w", err)
			}
			tables = append(tables, col)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("schema tables iteration failed: %w", err)
		}
		if len(tables) == agent.MaxDDLTableColumns {
			logger.Warnf("Schema snapshot: table columns truncated at %d rows", agent.MaxDDLTableColumns)
		}
		snapshot.Tables = tables

		// Collect indexes
		idxRows, err := utils.QueryWithPrefix(pool, ctx, schemaIndexesQuery, agent.MaxDDLIndexDefinitions)
		if err != nil {
			return nil, fmt.Errorf("schema indexes query failed: %w", err)
		}
		defer idxRows.Close()

		var indexes []agent.SchemaIndex
		for idxRows.Next() {
			var idx agent.SchemaIndex
			if err := idxRows.Scan(&idx.IndexID, &idx.IndexDefinition); err != nil {
				return nil, fmt.Errorf("schema indexes scan failed: %w", err)
			}
			indexes = append(indexes, idx)
		}
		if err := idxRows.Err(); err != nil {
			return nil, fmt.Errorf("schema indexes iteration failed: %w", err)
		}
		if len(indexes) == agent.MaxDDLIndexDefinitions {
			logger.Warnf("Schema snapshot: index definitions truncated at %d rows", agent.MaxDDLIndexDefinitions)
		}
		snapshot.Indexes = indexes
	}

	return snapshot, nil
}
