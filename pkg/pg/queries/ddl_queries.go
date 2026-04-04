package queries

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

// systemSchemaFilter is the WHERE clause fragment that excludes system schemas.
// Used by discoverSchemas to find user-created schemas.
const discoverSchemasQuery = `
SELECT nspname
FROM pg_namespace
WHERE nspname NOT LIKE 'pg_%'
  AND nspname <> 'information_schema'
ORDER BY nspname
`

// discoverSchemas returns all non-system schema names in the current database.
func discoverSchemas(pool *pgxpool.Pool, ctx context.Context) ([]string, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, discoverSchemasQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to discover schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan schema name: %w", err)
		}
		schemas = append(schemas, name)
	}
	return schemas, rows.Err()
}

// --------------------------------------------------------------------
// 1. Enum types referenced by table columns in the target schemas.
// --------------------------------------------------------------------
const ddlEnumTypesQuery = `
SELECT
    quote_ident(tn.nspname) AS schema_name,
    quote_ident(t.typname)  AS type_name,
    array_agg(e.enumlabel ORDER BY e.enumsortorder) AS labels
FROM pg_type t
JOIN pg_namespace tn ON tn.oid = t.typnamespace
JOIN pg_enum e ON e.enumtypid = t.oid
WHERE t.typtype = 'e'
  AND t.oid IN (
      SELECT DISTINCT a.atttypid
      FROM pg_attribute a
      JOIN pg_class c ON c.oid = a.attrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = ANY($1::text[]) AND c.relkind IN ('r','p')
        AND a.attnum > 0 AND NOT a.attisdropped
  )
GROUP BY tn.nspname, t.typname
ORDER BY tn.nspname, t.typname
`

func queryDDLEnumTypes(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLEnumType, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlEnumTypesQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL enum types: %w", err)
	}
	defer rows.Close()

	var result []DDLEnumType
	for rows.Next() {
		var e DDLEnumType
		if err := rows.Scan(&e.SchemaName, &e.TypeName, &e.Labels); err != nil {
			return nil, fmt.Errorf("failed to scan DDL enum type: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
// 2. Domain types referenced by table columns in the target schemas.
// --------------------------------------------------------------------
const ddlDomainTypesQuery = `
SELECT
    quote_ident(tn.nspname)              AS schema_name,
    quote_ident(t.typname)               AS type_name,
    format_type(t.typbasetype, t.typtypmod) AS base_type,
    CASE WHEN t.typnotnull AND NOT EXISTS (
        SELECT 1 FROM pg_constraint con
        WHERE con.contypid = t.oid AND pg_get_constraintdef(con.oid) LIKE 'NOT NULL%'
    ) THEN true ELSE false END           AS not_null,
    t.typdefault                         AS default_val,
    COALESCE(
        (SELECT array_agg(pg_get_constraintdef(con.oid))
         FROM pg_constraint con WHERE con.contypid = t.oid),
        '{}'
    ) AS constraint_defs
FROM pg_type t
JOIN pg_namespace tn ON tn.oid = t.typnamespace
WHERE t.typtype = 'd'
  AND t.oid IN (
      SELECT DISTINCT a.atttypid
      FROM pg_attribute a
      JOIN pg_class c ON c.oid = a.attrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = ANY($1::text[]) AND c.relkind IN ('r','p')
        AND a.attnum > 0 AND NOT a.attisdropped
  )
ORDER BY tn.nspname, t.typname
`

func queryDDLDomainTypes(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLDomainType, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlDomainTypesQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL domain types: %w", err)
	}
	defer rows.Close()

	var result []DDLDomainType
	for rows.Next() {
		var d DDLDomainType
		if err := rows.Scan(&d.SchemaName, &d.TypeName, &d.BaseType, &d.NotNull, &d.Default, &d.ConstraintDefs); err != nil {
			return nil, fmt.Errorf("failed to scan DDL domain type: %w", err)
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
// 2b. Composite types referenced by table columns in the target schemas.
// --------------------------------------------------------------------
const ddlCompositeTypesQuery = `
SELECT
    quote_ident(tn.nspname) AS schema_name,
    quote_ident(t.typname)  AS type_name,
    string_agg(
        quote_ident(a.attname) || ' ' || format_type(a.atttypid, a.atttypmod),
        ', ' ORDER BY a.attnum
    ) AS attributes
FROM pg_type t
JOIN pg_namespace tn ON tn.oid = t.typnamespace
JOIN pg_attribute a ON a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
WHERE t.typtype = 'c'
  AND t.typrelid <> 0
  AND (SELECT c.relkind FROM pg_class c WHERE c.oid = t.typrelid) = 'c'
  AND t.oid IN (
      SELECT DISTINCT col.atttypid
      FROM pg_attribute col
      JOIN pg_class c ON c.oid = col.attrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = ANY($1::text[]) AND c.relkind IN ('r','p')
        AND col.attnum > 0 AND NOT col.attisdropped
  )
GROUP BY tn.nspname, t.typname
ORDER BY tn.nspname, t.typname
`

func queryDDLCompositeTypes(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLCompositeType, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlCompositeTypesQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL composite types: %w", err)
	}
	defer rows.Close()

	var result []DDLCompositeType
	for rows.Next() {
		var ct DDLCompositeType
		if err := rows.Scan(&ct.SchemaName, &ct.TypeName, &ct.Attributes); err != nil {
			return nil, fmt.Errorf("failed to scan DDL composite type: %w", err)
		}
		result = append(result, ct)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
//  3. All sequences with classification (standalone/serial/identity).
//     Names are schema-qualified in the output.
//
// --------------------------------------------------------------------
const ddlAllSequencesQuery = `
SELECT
    quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS name,
    format_type(s.seqtypid, NULL)   AS data_type,
    s.seqstart                      AS start,
    s.seqincrement                  AS increment,
    s.seqmin                        AS min_value,
    s.seqmax                        AS max_value,
    s.seqcache                      AS cache,
    s.seqcycle                      AS cycle,
    CASE
        WHEN EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'i'
                     AND d.refclassid = 'pg_class'::regclass) THEN 'identity'
        WHEN EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'a'
                     AND d.refclassid = 'pg_class'::regclass) THEN 'serial'
        ELSE 'standalone'
    END AS seq_kind,
    (SELECT quote_ident(tn.nspname) || '.' || quote_ident(tc.relname)
     FROM pg_depend d
     JOIN pg_class tc ON tc.oid = d.refobjid
     JOIN pg_namespace tn ON tn.oid = tc.relnamespace
     WHERE d.objid = c.oid AND d.deptype IN ('a','i')
       AND d.refclassid = 'pg_class'::regclass
     LIMIT 1) AS owner_table,
    (SELECT quote_ident(att.attname) FROM pg_depend d
     JOIN pg_attribute att ON att.attrelid = d.refobjid AND att.attnum = d.refobjsubid
     WHERE d.objid = c.oid AND d.deptype IN ('a','i')
       AND d.refclassid = 'pg_class'::regclass AND d.refobjsubid > 0
     LIMIT 1) AS owner_column,
    (SELECT att.attidentity::text FROM pg_depend d
     JOIN pg_attribute att ON att.attrelid = d.refobjid AND att.attnum = d.refobjsubid
     WHERE d.objid = c.oid AND d.deptype = 'i'
       AND d.refclassid = 'pg_class'::regclass AND d.refobjsubid > 0
     LIMIT 1) AS identity_kind,
    (SELECT pg_get_expr(ad.adbin, ad.adrelid) FROM pg_depend d
     JOIN pg_attrdef ad ON ad.adrelid = d.refobjid AND ad.adnum = d.refobjsubid
     WHERE d.objid = c.oid AND d.deptype = 'a'
       AND d.refclassid = 'pg_class'::regclass AND d.refobjsubid > 0
     LIMIT 1) AS default_expr,
    (SELECT tc.relkind::text FROM pg_depend d
     JOIN pg_class tc ON tc.oid = d.refobjid
     WHERE d.objid = c.oid AND d.deptype IN ('a','i')
       AND d.refclassid = 'pg_class'::regclass
     LIMIT 1) AS owner_relkind
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_sequence s ON s.seqrelid = c.oid
WHERE n.nspname = ANY($1::text[]) AND c.relkind = 'S'
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e'
  )
ORDER BY n.nspname, c.relname
`

func queryDDLAllSequences(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLSequenceRaw, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlAllSequencesQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL sequences: %w", err)
	}
	defer rows.Close()

	var result []DDLSequenceRaw
	for rows.Next() {
		var s DDLSequenceRaw
		if err := rows.Scan(
			&s.Name, &s.DataType, &s.Start, &s.Increment,
			&s.MinValue, &s.MaxValue, &s.Cache, &s.Cycle,
			&s.Kind, &s.OwnerTable, &s.OwnerColumn,
			&s.IdentityKind, &s.DefaultExpr, &s.OwnerRelKind,
		); err != nil {
			return nil, fmt.Errorf("failed to scan DDL sequence: %w", err)
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

// classifySequences splits raw sequences into standalone, serial, and identity.
func classifySequences(raw []DDLSequenceRaw) ([]SeqParams, []DDLSerialSequence, []DDLIdentitySequence) {
	var standalone []SeqParams
	var serial []DDLSerialSequence
	var identity []DDLIdentitySequence

	for _, s := range raw {
		switch s.Kind {
		case "serial":
			if s.OwnerTable != nil && s.OwnerColumn != nil && s.DefaultExpr != nil {
				serial = append(serial, DDLSerialSequence{
					SeqParams:   s.SeqParams,
					TableName:   *s.OwnerTable,
					ColumnName:  *s.OwnerColumn,
					DefaultExpr: *s.DefaultExpr,
				})
			}
		case "identity":
			if s.OwnerTable != nil && s.OwnerColumn != nil && s.IdentityKind != nil {
				identity = append(identity, DDLIdentitySequence{
					SeqParams:    s.SeqParams,
					TableName:    *s.OwnerTable,
					ColumnName:   *s.OwnerColumn,
					IdentityKind: *s.IdentityKind,
					Partitioned:  s.OwnerRelKind != nil && *s.OwnerRelKind == "p",
				})
			}
		default:
			standalone = append(standalone, s.SeqParams)
		}
	}
	return standalone, serial, identity
}

// --------------------------------------------------------------------
//  4. Functions and procedures in the target schemas.
//     pg_get_functiondef already includes the schema-qualified name.
//
// --------------------------------------------------------------------
const ddlFunctionsQuery = `
SELECT
    p.oid::integer   AS oid,
    p.proname        AS name,
    pg_get_functiondef(p.oid) AS function_def
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = ANY($1::text[])
  AND p.prokind IN ('f', 'p')
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.objid = p.oid AND d.deptype = 'e'
  )
ORDER BY n.nspname, p.proname, p.oid
`

func queryDDLFunctions(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLFunction, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlFunctionsQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL functions: %w", err)
	}
	defer rows.Close()

	var result []DDLFunction
	for rows.Next() {
		var f DDLFunction
		if err := rows.Scan(&f.OID, &f.Name, &f.FunctionDef); err != nil {
			return nil, fmt.Errorf("failed to scan DDL function: %w", err)
		}
		result = append(result, f)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
//  5. Tables (metadata only — columns and constraints are separate).
//     Names are schema-qualified in the output.
//
// --------------------------------------------------------------------
const ddlTablesQuery = `
SELECT
    c.oid::integer         AS oid,
    c.relname              AS name,
    quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS quoted_name,
    c.relkind::text        AS relkind,
    c.relpersistence::text AS persistence,
    CASE WHEN c.reloptions IS NOT NULL
        THEN array_to_string(c.reloptions, ', ') END AS rel_options,
    CASE WHEN c.reltablespace <> 0
        THEN quote_ident((SELECT spcname FROM pg_tablespace WHERE oid = c.reltablespace))
    END AS tablespace,
    CASE WHEN c.relkind = 'p'
        THEN pg_get_partkeydef(c.oid) END AS partition_key,
    (SELECT string_agg(
         quote_ident(pn.nspname) || '.' || quote_ident(pc.relname),
         ', ' ORDER BY i.inhseqno)
     FROM pg_inherits i
     JOIN pg_class pc ON pc.oid = i.inhparent
     JOIN pg_namespace pn ON pn.oid = pc.relnamespace
     WHERE i.inhrelid = c.oid AND pc.relkind = 'r'
    ) AS inherits_from
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
  AND c.relkind IN ('r', 'p')
  AND NOT EXISTS (
      SELECT 1 FROM pg_inherits i
      JOIN pg_class pc ON pc.oid = i.inhparent
      WHERE i.inhrelid = c.oid AND pc.relkind = 'p'
  )
ORDER BY n.nspname, c.relname
`

func queryDDLTables(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLTable, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlTablesQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL tables: %w", err)
	}
	defer rows.Close()

	var result []DDLTable
	for rows.Next() {
		var t DDLTable
		if err := rows.Scan(
			&t.OID, &t.Name, &t.QuotedName, &t.RelKind, &t.Persistence,
			&t.RelOptions, &t.Tablespace, &t.PartitionKey, &t.InheritsFrom,
		); err != nil {
			return nil, fmt.Errorf("failed to scan DDL table: %w", err)
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
//  6. Table columns — includes storage and statistics overrides.
//     TableName is schema-qualified for use in ALTER TABLE statements.
//
// --------------------------------------------------------------------
const ddlColumnsQuery = `
SELECT
    c.oid::integer          AS table_oid,
    quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS table_name,
    a.attnum                AS attnum,
    quote_ident(a.attname)  AS name,
    format_type(a.atttypid, a.atttypmod) AS type_name,
    CASE WHEN a.attcollation <> 0
        AND a.attcollation <> (SELECT typcollation FROM pg_type WHERE oid = a.atttypid)
        THEN quote_ident((SELECT collname FROM pg_collation WHERE oid = a.attcollation))
    END AS collation,
    CASE WHEN a.attidentity = '' THEN NULL ELSE a.attidentity::text END AS identity,
    CASE WHEN a.attgenerated = '' THEN NULL ELSE a.attgenerated::text END AS generated,
    a.attnotnull            AS not_null,
    CASE WHEN d.adbin IS NOT NULL
        THEN pg_get_expr(d.adbin, d.adrelid) END AS default_expr,
    CASE WHEN a.attstorage <> (SELECT t.typstorage FROM pg_type t WHERE t.oid = a.atttypid)
        THEN a.attstorage::text END AS storage,
    CASE WHEN a.attstattarget >= 0
        THEN a.attstattarget END AS statistics
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = ANY($1::text[])
  AND c.relkind IN ('r', 'p')
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND NOT EXISTS (
      SELECT 1 FROM pg_inherits i
      JOIN pg_class pc ON pc.oid = i.inhparent
      WHERE i.inhrelid = c.oid AND pc.relkind = 'p'
  )
  AND (a.attislocal OR a.attinhcount = 0)
ORDER BY c.oid, a.attnum
`

func queryDDLColumns(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLColumn, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlColumnsQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL columns: %w", err)
	}
	defer rows.Close()

	var result []DDLColumn
	for rows.Next() {
		var col DDLColumn
		if err := rows.Scan(
			&col.TableOID, &col.TableName, &col.AttNum, &col.Name, &col.TypeName,
			&col.Collation, &col.Identity, &col.Generated,
			&col.NotNull, &col.DefaultExpr, &col.Storage, &col.Statistics,
		); err != nil {
			return nil, fmt.Errorf("failed to scan DDL column: %w", err)
		}
		result = append(result, col)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
//  7. Table constraints (PK, UNIQUE, CHECK, EXCLUDE — not FK).
//     Table names are schema-qualified.
//
// --------------------------------------------------------------------
const ddlConstraintsQuery = `
SELECT
    c.relname                AS table_name,
    quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS quoted_table,
    con.conname              AS con_name,
    quote_ident(con.conname) AS quoted_con_name,
    con.contype::text        AS con_type,
    pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
  AND con.contype IN ('p', 'u', 'c', 'x')
ORDER BY n.nspname, c.relname, con.conname
`

func queryDDLConstraints(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLConstraint, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlConstraintsQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL constraints: %w", err)
	}
	defer rows.Close()

	var result []DDLConstraint
	for rows.Next() {
		var c DDLConstraint
		if err := rows.Scan(&c.TableName, &c.QuotedTable, &c.ConName, &c.QuotedConName, &c.ConType, &c.ConstraintDef); err != nil {
			return nil, fmt.Errorf("failed to scan DDL constraint: %w", err)
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
// 8. Partition children. Names are schema-qualified.
// --------------------------------------------------------------------
const ddlPartitionChildrenQuery = `
SELECT
    quote_ident(n.nspname) || '.' || quote_ident(c.relname)  AS child_name,
    quote_ident(pn.nspname) || '.' || quote_ident(pc.relname) AS parent_name,
    pg_get_expr(c.relpartbound, c.oid) AS bound_expr
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class pc ON pc.oid = i.inhparent
JOIN pg_namespace pn ON pn.oid = pc.relnamespace
WHERE n.nspname = ANY($1::text[])
  AND pc.relkind = 'p'
ORDER BY pn.nspname, pc.relname, c.relname
`

func queryDDLPartitionChildren(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLPartitionChild, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlPartitionChildrenQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL partition children: %w", err)
	}
	defer rows.Close()

	var result []DDLPartitionChild
	for rows.Next() {
		var p DDLPartitionChild
		if err := rows.Scan(&p.ChildName, &p.ParentName, &p.BoundExpr); err != nil {
			return nil, fmt.Errorf("failed to scan DDL partition child: %w", err)
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
//  9. Indexes (excludes constraint-backing indexes).
//     indexdef from pg_indexes already includes schema-qualified names.
//
// --------------------------------------------------------------------
const ddlIndexesQuery = `
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = ANY($1::text[])
  AND indexname NOT IN (
      SELECT c.relname
      FROM pg_constraint con
      JOIN pg_class c ON c.oid = con.conindid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = ANY($1::text[])
  )
ORDER BY schemaname, indexname
`

func queryDDLIndexes(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLIndex, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlIndexesQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL indexes: %w", err)
	}
	defer rows.Close()

	var result []DDLIndex
	for rows.Next() {
		var idx DDLIndex
		if err := rows.Scan(&idx.IndexName, &idx.IndexDef); err != nil {
			return nil, fmt.Errorf("failed to scan DDL index: %w", err)
		}
		result = append(result, idx)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
//  10. Foreign key constraints (emitted as ALTER TABLE after all tables).
//     Table names are schema-qualified.
//
// --------------------------------------------------------------------
const ddlForeignKeysQuery = `
SELECT
    quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS table_name,
    quote_ident(con.conname)  AS con_name,
    pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
  AND con.contype = 'f'
ORDER BY n.nspname, c.relname, con.conname
`

func queryDDLForeignKeys(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLForeignKey, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlForeignKeysQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL foreign keys: %w", err)
	}
	defer rows.Close()

	var result []DDLForeignKey
	for rows.Next() {
		var fk DDLForeignKey
		if err := rows.Scan(&fk.TableName, &fk.ConName, &fk.ConstraintDef); err != nil {
			return nil, fmt.Errorf("failed to scan DDL foreign key: %w", err)
		}
		result = append(result, fk)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
// 11. Views (excludes extension-owned). Names are schema-qualified.
// --------------------------------------------------------------------
const ddlViewsQuery = `
SELECT
    quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS name,
    pg_get_viewdef(c.oid)  AS view_def,
    CASE WHEN c.reloptions IS NOT NULL
        THEN array_to_string(c.reloptions, ', ') END AS rel_options
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[]) AND c.relkind = 'v'
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.objid = c.oid AND d.deptype = 'e'
  )
ORDER BY n.nspname, c.relname
`

func queryDDLViews(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLView, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlViewsQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL views: %w", err)
	}
	defer rows.Close()

	var result []DDLView
	for rows.Next() {
		var v DDLView
		if err := rows.Scan(&v.Name, &v.ViewDef, &v.RelOptions); err != nil {
			return nil, fmt.Errorf("failed to scan DDL view: %w", err)
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
// 12. Materialized views. Names are schema-qualified.
// --------------------------------------------------------------------
const ddlMaterializedViewsQuery = `
SELECT
    quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS name,
    pg_get_viewdef(c.oid)  AS view_def,
    CASE WHEN c.reloptions IS NOT NULL
        THEN array_to_string(c.reloptions, ', ') END AS rel_options,
    CASE WHEN c.reltablespace <> 0
        THEN quote_ident((SELECT spcname FROM pg_tablespace WHERE oid = c.reltablespace))
    END AS tablespace
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[]) AND c.relkind = 'm'
ORDER BY n.nspname, c.relname
`

func queryDDLMaterializedViews(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLMaterializedView, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlMaterializedViewsQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL materialized views: %w", err)
	}
	defer rows.Close()

	var result []DDLMaterializedView
	for rows.Next() {
		var mv DDLMaterializedView
		if err := rows.Scan(&mv.Name, &mv.ViewDef, &mv.RelOptions, &mv.Tablespace); err != nil {
			return nil, fmt.Errorf("failed to scan DDL materialized view: %w", err)
		}
		result = append(result, mv)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
// 13. Tables with row-level security enabled. Names are schema-qualified.
// --------------------------------------------------------------------
const ddlRLSEnabledQuery = `
SELECT
    quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS table_name,
    c.relforcerowsecurity AS force
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[]) AND c.relrowsecurity
ORDER BY n.nspname, c.relname
`

func queryDDLRLSEnabled(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLRLSEnabled, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlRLSEnabledQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL RLS enabled: %w", err)
	}
	defer rows.Close()

	var result []DDLRLSEnabled
	for rows.Next() {
		var r DDLRLSEnabled
		if err := rows.Scan(&r.TableName, &r.Force); err != nil {
			return nil, fmt.Errorf("failed to scan DDL RLS enabled: %w", err)
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
// 14. Row-level security policies (with roles). Names are schema-qualified.
// --------------------------------------------------------------------
const ddlRLSPoliciesQuery = `
SELECT
    quote_ident(pol.polname)  AS policy_name,
    quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS table_name,
    pol.polcmd::text          AS command,
    pol.polpermissive         AS permissive,
    CASE WHEN pol.polroles = '{0}' THEN ARRAY['PUBLIC']
         ELSE (SELECT array_agg(quote_ident(rolname) ORDER BY rolname)
               FROM pg_roles WHERE oid = ANY(pol.polroles))
    END AS roles,
    pg_get_expr(pol.polqual, pol.polrelid)      AS using_expr,
    pg_get_expr(pol.polwithcheck, pol.polrelid)  AS with_check
FROM pg_policy pol
JOIN pg_class c ON c.oid = pol.polrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
ORDER BY n.nspname, c.relname, pol.polname
`

func queryDDLRLSPolicies(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLRLSPolicy, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlRLSPoliciesQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL RLS policies: %w", err)
	}
	defer rows.Close()

	var result []DDLRLSPolicy
	for rows.Next() {
		var p DDLRLSPolicy
		if err := rows.Scan(&p.PolicyName, &p.TableName, &p.Command, &p.Permissive, &p.Roles, &p.UsingExpr, &p.WithCheck); err != nil {
			return nil, fmt.Errorf("failed to scan DDL RLS policy: %w", err)
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------
//  15. Triggers (excluding internal/system triggers).
//     pg_get_triggerdef already includes schema-qualified names.
//
// --------------------------------------------------------------------
const ddlTriggersQuery = `
SELECT
    c.relname                AS table_name,
    t.tgname                 AS trig_name,
    pg_get_triggerdef(t.oid) AS trigger_def
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
  AND NOT t.tgisinternal
ORDER BY n.nspname, c.relname, t.tgname
`

func queryDDLTriggers(pool *pgxpool.Pool, ctx context.Context, schemas []string) ([]DDLTrigger, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlTriggersQuery, schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query DDL triggers: %w", err)
	}
	defer rows.Close()

	var result []DDLTrigger
	for rows.Next() {
		var tr DDLTrigger
		if err := rows.Scan(&tr.TableName, &tr.TrigName, &tr.TriggerDef); err != nil {
			return nil, fmt.Errorf("failed to scan DDL trigger: %w", err)
		}
		result = append(result, tr)
	}
	return result, rows.Err()
}
