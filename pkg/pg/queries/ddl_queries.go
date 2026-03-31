package queries

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --------------------------------------------------------------------
// 1. Enum types referenced by public-schema table columns.
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
      WHERE n.nspname = 'public' AND c.relkind IN ('r','p')
        AND a.attnum > 0 AND NOT a.attisdropped
  )
GROUP BY tn.nspname, t.typname
ORDER BY tn.nspname, t.typname
`

func queryDDLEnumTypes(pool *pgxpool.Pool, ctx context.Context) ([]DDLEnumType, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlEnumTypesQuery)
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
// 2. Domain types referenced by public-schema table columns.
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
      WHERE n.nspname = 'public' AND c.relkind IN ('r','p')
        AND a.attnum > 0 AND NOT a.attisdropped
  )
ORDER BY tn.nspname, t.typname
`

func queryDDLDomainTypes(pool *pgxpool.Pool, ctx context.Context) ([]DDLDomainType, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlDomainTypesQuery)
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
// 2b. Composite types referenced by public-schema table columns.
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
      WHERE n.nspname = 'public' AND c.relkind IN ('r','p')
        AND col.attnum > 0 AND NOT col.attisdropped
  )
GROUP BY tn.nspname, t.typname
ORDER BY tn.nspname, t.typname
`

func queryDDLCompositeTypes(pool *pgxpool.Pool, ctx context.Context) ([]DDLCompositeType, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlCompositeTypesQuery)
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
// 3. All sequences with classification (standalone/serial/identity).
// --------------------------------------------------------------------
const ddlAllSequencesQuery = `
SELECT
    quote_ident(c.relname)          AS name,
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
    (SELECT quote_ident(tc.relname) FROM pg_depend d
     JOIN pg_class tc ON tc.oid = d.refobjid
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
WHERE n.nspname = 'public' AND c.relkind = 'S'
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e'
  )
ORDER BY c.relname
`

func queryDDLAllSequences(pool *pgxpool.Pool, ctx context.Context) ([]DDLSequenceRaw, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlAllSequencesQuery)
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
// 4. Functions and procedures in the public schema.
// --------------------------------------------------------------------
const ddlFunctionsQuery = `
SELECT
    p.oid::integer   AS oid,
    p.proname        AS name,
    pg_get_functiondef(p.oid) AS function_def
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'public'
  AND p.prokind IN ('f', 'p')
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.objid = p.oid AND d.deptype = 'e'
  )
ORDER BY p.proname, p.oid
`

func queryDDLFunctions(pool *pgxpool.Pool, ctx context.Context) ([]DDLFunction, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlFunctionsQuery)
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
// 5. Tables (metadata only — columns and constraints are separate).
// --------------------------------------------------------------------
const ddlTablesQuery = `
SELECT
    c.oid::integer         AS oid,
    c.relname              AS name,
    quote_ident(c.relname) AS quoted_name,
    c.relkind::text        AS relkind,
    c.relpersistence::text AS persistence,
    CASE WHEN c.reloptions IS NOT NULL
        THEN array_to_string(c.reloptions, ', ') END AS rel_options,
    CASE WHEN c.reltablespace <> 0
        THEN quote_ident((SELECT spcname FROM pg_tablespace WHERE oid = c.reltablespace))
    END AS tablespace,
    CASE WHEN c.relkind = 'p'
        THEN pg_get_partkeydef(c.oid) END AS partition_key,
    (SELECT string_agg(quote_ident(pc.relname), ', ' ORDER BY i.inhseqno)
     FROM pg_inherits i
     JOIN pg_class pc ON pc.oid = i.inhparent
     WHERE i.inhrelid = c.oid AND pc.relkind = 'r'
    ) AS inherits_from
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind IN ('r', 'p')
  AND NOT EXISTS (
      SELECT 1 FROM pg_inherits i
      JOIN pg_class pc ON pc.oid = i.inhparent
      WHERE i.inhrelid = c.oid AND pc.relkind = 'p'
  )
ORDER BY c.relname
`

func queryDDLTables(pool *pgxpool.Pool, ctx context.Context) ([]DDLTable, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlTablesQuery)
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
//     In decomposed mode, defaults for serial columns and identity are
//     suppressed here; they're emitted as separate ALTER statements.
//
// --------------------------------------------------------------------
const ddlColumnsQuery = `
SELECT
    c.oid::integer          AS table_oid,
    quote_ident(c.relname)  AS table_name,
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
WHERE n.nspname = 'public'
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

func queryDDLColumns(pool *pgxpool.Pool, ctx context.Context) ([]DDLColumn, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlColumnsQuery)
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
// 7. Table constraints (PK, UNIQUE, CHECK, EXCLUDE — not FK).
// --------------------------------------------------------------------
const ddlConstraintsQuery = `
SELECT
    c.relname                AS table_name,
    quote_ident(c.relname)   AS quoted_table,
    con.conname              AS con_name,
    quote_ident(con.conname) AS quoted_con_name,
    con.contype::text        AS con_type,
    pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND con.contype IN ('p', 'u', 'c', 'x')
ORDER BY c.relname, con.conname
`

func queryDDLConstraints(pool *pgxpool.Pool, ctx context.Context) ([]DDLConstraint, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlConstraintsQuery)
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
// 8. Partition children.
// --------------------------------------------------------------------
const ddlPartitionChildrenQuery = `
SELECT
    quote_ident(c.relname)  AS child_name,
    quote_ident(pc.relname) AS parent_name,
    pg_get_expr(c.relpartbound, c.oid) AS bound_expr
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class pc ON pc.oid = i.inhparent
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND pc.relkind = 'p'
ORDER BY pc.relname, c.relname
`

func queryDDLPartitionChildren(pool *pgxpool.Pool, ctx context.Context) ([]DDLPartitionChild, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlPartitionChildrenQuery)
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
// 9. Indexes (excludes constraint-backing indexes).
// --------------------------------------------------------------------
const ddlIndexesQuery = `
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND indexname NOT IN (
      SELECT c.relname
      FROM pg_constraint con
      JOIN pg_class c ON c.oid = con.conindid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public'
  )
ORDER BY indexname
`

func queryDDLIndexes(pool *pgxpool.Pool, ctx context.Context) ([]DDLIndex, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlIndexesQuery)
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
// 10. Foreign key constraints (emitted as ALTER TABLE after all tables).
// --------------------------------------------------------------------
const ddlForeignKeysQuery = `
SELECT
    quote_ident(c.relname)    AS table_name,
    quote_ident(con.conname)  AS con_name,
    pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND con.contype = 'f'
ORDER BY c.relname, con.conname
`

func queryDDLForeignKeys(pool *pgxpool.Pool, ctx context.Context) ([]DDLForeignKey, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlForeignKeysQuery)
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
// 11. Views (excludes extension-owned).
// --------------------------------------------------------------------
const ddlViewsQuery = `
SELECT
    quote_ident(c.relname) AS name,
    pg_get_viewdef(c.oid)  AS view_def,
    CASE WHEN c.reloptions IS NOT NULL
        THEN array_to_string(c.reloptions, ', ') END AS rel_options
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'v'
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.objid = c.oid AND d.deptype = 'e'
  )
ORDER BY c.relname
`

func queryDDLViews(pool *pgxpool.Pool, ctx context.Context) ([]DDLView, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlViewsQuery)
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
// 12. Materialized views.
// --------------------------------------------------------------------
const ddlMaterializedViewsQuery = `
SELECT
    quote_ident(c.relname) AS name,
    pg_get_viewdef(c.oid)  AS view_def,
    CASE WHEN c.reloptions IS NOT NULL
        THEN array_to_string(c.reloptions, ', ') END AS rel_options,
    CASE WHEN c.reltablespace <> 0
        THEN quote_ident((SELECT spcname FROM pg_tablespace WHERE oid = c.reltablespace))
    END AS tablespace
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'm'
ORDER BY c.relname
`

func queryDDLMaterializedViews(pool *pgxpool.Pool, ctx context.Context) ([]DDLMaterializedView, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlMaterializedViewsQuery)
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
// 13. Tables with row-level security enabled.
// --------------------------------------------------------------------
const ddlRLSEnabledQuery = `
SELECT quote_ident(c.relname) AS table_name, c.relforcerowsecurity AS force
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relrowsecurity
ORDER BY c.relname
`

func queryDDLRLSEnabled(pool *pgxpool.Pool, ctx context.Context) ([]DDLRLSEnabled, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlRLSEnabledQuery)
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
// 14. Row-level security policies (with roles).
// --------------------------------------------------------------------
const ddlRLSPoliciesQuery = `
SELECT
    quote_ident(pol.polname)  AS policy_name,
    quote_ident(c.relname)    AS table_name,
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
WHERE n.nspname = 'public'
ORDER BY c.relname, pol.polname
`

func queryDDLRLSPolicies(pool *pgxpool.Pool, ctx context.Context) ([]DDLRLSPolicy, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlRLSPoliciesQuery)
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
// 15. Triggers (excluding internal/system triggers).
// --------------------------------------------------------------------
const ddlTriggersQuery = `
SELECT
    c.relname                AS table_name,
    t.tgname                 AS trig_name,
    pg_get_triggerdef(t.oid) AS trigger_def
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND NOT t.tgisinternal
ORDER BY c.relname, t.tgname
`

func queryDDLTriggers(pool *pgxpool.Pool, ctx context.Context) ([]DDLTrigger, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, ddlTriggersQuery)
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
