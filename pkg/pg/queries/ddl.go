package queries

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultDDLTimeout is the maximum time allowed for the full DDL collection.
const DefaultDDLTimeout = 30 * time.Second

// HashDDL computes a SHA-256 hash of a DDL string for change detection.
func HashDDL(ddl string) string {
	hash := sha256.Sum256([]byte(ddl))
	return fmt.Sprintf("%x", hash)
}

// CollectDDL queries the PostgreSQL catalog and reconstructs a complete DDL
// snapshot in pg_dump's decomposed format. The output is ordered:
//
//	Pre-data:  extensions, schemas, types, functions, sequences, tables (bare),
//	           partitions, serial defaults, identity, sequence ownership,
//	           storage/statistics overrides, views
//	Post-data: constraints, foreign keys, indexes, triggers, RLS
func CollectDDL(pgPool *pgxpool.Pool, ctx context.Context) (string, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultDDLTimeout)
		defer cancel()
	}

	// Run all catalog queries.
	enums, err := queryDDLEnumTypes(pgPool, ctx)
	if err != nil {
		return "", err
	}
	domains, err := queryDDLDomainTypes(pgPool, ctx)
	if err != nil {
		return "", err
	}
	composites, err := queryDDLCompositeTypes(pgPool, ctx)
	if err != nil {
		return "", err
	}
	functions, err := queryDDLFunctions(pgPool, ctx)
	if err != nil {
		return "", err
	}
	tables, err := queryDDLTables(pgPool, ctx)
	if err != nil {
		return "", err
	}
	columns, err := queryDDLColumns(pgPool, ctx)
	if err != nil {
		return "", err
	}
	constraints, err := queryDDLConstraints(pgPool, ctx)
	if err != nil {
		return "", err
	}
	allSeqsRaw, err := queryDDLAllSequences(pgPool, ctx)
	if err != nil {
		return "", err
	}
	partitions, err := queryDDLPartitionChildren(pgPool, ctx)
	if err != nil {
		return "", err
	}
	indexes, err := queryDDLIndexes(pgPool, ctx)
	if err != nil {
		return "", err
	}
	fks, err := queryDDLForeignKeys(pgPool, ctx)
	if err != nil {
		return "", err
	}
	views, err := queryDDLViews(pgPool, ctx)
	if err != nil {
		return "", err
	}
	matviews, err := queryDDLMaterializedViews(pgPool, ctx)
	if err != nil {
		return "", err
	}
	rlsEnabled, err := queryDDLRLSEnabled(pgPool, ctx)
	if err != nil {
		return "", err
	}
	rlsPolicies, err := queryDDLRLSPolicies(pgPool, ctx)
	if err != nil {
		return "", err
	}
	triggers, err := queryDDLTriggers(pgPool, ctx)
	if err != nil {
		return "", err
	}

	// Classify sequences into standalone, serial-backing, identity-backing.
	standaloneSeqs, serialSeqs, identitySeqs := classifySequences(allSeqsRaw)

	// Build lookup maps for table formatting.
	columnsByOID := groupBy(columns, func(c DDLColumn) uint32 { return c.TableOID })
	serialCols := colSet(serialSeqs, func(s DDLSerialSequence) (string, string) { return s.TableName, s.ColumnName })
	identityCols := colSet(identitySeqs, func(s DDLIdentitySequence) (string, string) { return s.TableName, s.ColumnName })
	pkCols, err := buildPKColSet(pgPool, ctx)
	if err != nil {
		return "", err
	}

	// Filter columns with storage/statistics overrides.
	storageOverrides := filter(columns, func(c DDLColumn) bool { return c.Storage != nil })
	statsOverrides := filter(columns, func(c DDLColumn) bool { return c.Statistics != nil })

	var b strings.Builder

	// ================================================================
	// PRE-DATA
	// ================================================================

	// Extensions.
	writeExtensions(&b, pgPool, ctx)

	// Schemas for cross-schema types.
	writeNonPublicSchemas(&b, enums, domains, composites)

	// Types.
	emit(&b, enums, formatDDLEnumType)
	emit(&b, domains, formatDDLDomainType)
	emit(&b, composites, formatDDLCompositeType)

	// Functions (extra blank line between each).
	emitSep(&b, functions, formatDDLFunction, "\n")

	// Sequences (standalone + serial-backing; identity sequences are created by ALTER TABLE).
	emit(&b, standaloneSeqs, formatCreateSequence)
	emit(&b, serialSeqs, formatSerialCreateSequence)

	// Tables (bare columns, no defaults for serial/identity, no constraints).
	for _, t := range tables {
		cols := columnsByOID[t.OID]
		b.WriteString("\n")
		b.WriteString(formatDDLTable(&t, cols, serialCols, identityCols, pkCols))
	}

	// Partition children.
	emit(&b, partitions, formatDDLPartitionChild)

	// Serial defaults + identity + sequence ownership.
	emit(&b, serialSeqs, formatSerialDefault)
	emit(&b, identitySeqs, formatIdentityColumn)
	emit(&b, serialSeqs, formatSequenceOwnedBy)

	// Storage and statistics overrides.
	emit(&b, storageOverrides, formatColumnSetStorage)
	emit(&b, statsOverrides, formatColumnSetStatistics)

	// Views.
	emit(&b, views, formatDDLView)
	emit(&b, matviews, formatDDLMaterializedView)

	// ================================================================
	// POST-DATA
	// ================================================================

	emit(&b, constraints, formatDDLConstraint)
	emit(&b, fks, formatDDLForeignKey)
	emit(&b, indexes, formatDDLIndex)
	emit(&b, triggers, formatDDLTrigger)
	emit(&b, rlsEnabled, formatDDLRLSEnable)
	emit(&b, rlsPolicies, formatDDLRLSPolicy)

	return b.String(), nil
}

// ---------------------------------------------------------------------------
// Generic helpers
// ---------------------------------------------------------------------------

// emit writes each item formatted with a newline before and after.
func emit[T any](b *strings.Builder, items []T, format func(*T) string) {
	for i := range items {
		b.WriteString("\n")
		b.WriteString(format(&items[i]))
		b.WriteString("\n")
	}
}

// emitSep writes items with an extra separator line between them.
func emitSep[T any](b *strings.Builder, items []T, format func(*T) string, extraSep string) {
	for i := range items {
		b.WriteString("\n")
		b.WriteString(format(&items[i]))
		b.WriteString("\n")
		if i < len(items)-1 {
			b.WriteString(extraSep)
		}
	}
}

// groupBy groups a slice by a key function.
func groupBy[T any, K comparable](items []T, key func(T) K) map[K][]T {
	m := make(map[K][]T)
	for _, item := range items {
		k := key(item)
		m[k] = append(m[k], item)
	}
	return m
}

// colSet builds a set of "tableName.colName" from items with a table/column accessor.
func colSet[T any](items []T, accessor func(T) (string, string)) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, item := range items {
		table, col := accessor(item)
		m[table+"."+col] = true
	}
	return m
}

// filter returns items matching a predicate.
func filter[T any](items []T, pred func(T) bool) []T {
	var out []T
	for _, item := range items {
		if pred(item) {
			out = append(out, item)
		}
	}
	return out
}

// buildPKColSet queries for PK column positions and returns a set of
// "tableOID:attnum" keys.
func buildPKColSet(pool *pgxpool.Pool, ctx context.Context) (map[string]bool, error) {
	rows, err := pool.Query(ctx, `
		SELECT con.conrelid::integer, unnest(con.conkey)::integer
		FROM pg_constraint con
		JOIN pg_namespace n ON n.oid = (SELECT relnamespace FROM pg_class WHERE oid = con.conrelid)
		WHERE n.nspname = 'public' AND con.contype = 'p'`)
	if err != nil {
		return nil, fmt.Errorf("pk columns: %w", err)
	}
	defer rows.Close()

	m := make(map[string]bool)
	for rows.Next() {
		var tableOID, attnum int
		if err := rows.Scan(&tableOID, &attnum); err != nil {
			return nil, err
		}
		m[fmt.Sprintf("%d:%d", tableOID, attnum)] = true
	}
	return m, rows.Err()
}

// writeExtensions emits CREATE EXTENSION statements for public-schema extensions.
func writeExtensions(b *strings.Builder, pool *pgxpool.Pool, ctx context.Context) {
	rows, _ := pool.Query(ctx, `
		SELECT 'CREATE EXTENSION IF NOT EXISTS ' || quote_ident(e.extname) ||
		       ' WITH SCHEMA ' || quote_ident(n.nspname) || ';'
		FROM pg_extension e
		JOIN pg_namespace n ON n.oid = e.extnamespace
		WHERE n.nspname = 'public' AND e.extname <> 'plpgsql'
		ORDER BY e.extname`)
	if rows == nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var stmt string
		if rows.Scan(&stmt) == nil {
			b.WriteString(stmt)
			b.WriteString("\n\n")
		}
	}
}

// writeNonPublicSchemas emits CREATE SCHEMA for any non-public schema
// referenced by types (enums, domains, composites).
func writeNonPublicSchemas(b *strings.Builder, enums []DDLEnumType, domains []DDLDomainType, composites []DDLCompositeType) {
	seen := make(map[string]bool)
	emitSchema := func(schema string) {
		if schema != "public" && !seen[schema] {
			seen[schema] = true
			b.WriteString("CREATE SCHEMA IF NOT EXISTS ")
			b.WriteString(schema)
			b.WriteString(";\n\n")
		}
	}
	for _, e := range enums {
		emitSchema(e.SchemaName)
	}
	for _, d := range domains {
		emitSchema(d.SchemaName)
	}
	for _, ct := range composites {
		emitSchema(ct.SchemaName)
	}
}
