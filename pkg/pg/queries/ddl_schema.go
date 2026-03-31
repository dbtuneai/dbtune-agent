package queries

// DDL schema structs — one per catalog object type.
// These model pg_dump's decomposed output format where tables are bare
// (columns + NOT NULL only) and defaults, constraints, identity, and
// sequence ownership are emitted as separate ALTER statements.

// DDLEnumType represents a user-defined ENUM type referenced by public-schema tables.
type DDLEnumType struct {
	SchemaName string   // quote_ident(tn.nspname)
	TypeName   string   // quote_ident(t.typname)
	Labels     []string // ordered enum labels (raw, need quote_literal in formatter)
}

// DDLDomainType represents a DOMAIN type referenced by public-schema tables.
type DDLDomainType struct {
	SchemaName     string   // quote_ident(tn.nspname)
	TypeName       string   // quote_ident(t.typname)
	BaseType       string   // format_type(t.typbasetype, t.typtypmod)
	NotNull        bool     // t.typnotnull
	Default        *string  // t.typdefault (nil = no default)
	ConstraintDefs []string // pg_get_constraintdef() for each domain constraint
}

// DDLCompositeType represents a composite type referenced by public-schema tables.
type DDLCompositeType struct {
	SchemaName string // quote_ident(tn.nspname)
	TypeName   string // quote_ident(t.typname)
	Attributes string // formatted attribute list: "field1 type1, field2 type2, ..."
}

// SeqParams holds the common parameters shared by all sequence types.
type SeqParams struct {
	Name      string
	DataType  string
	Start     int64
	Increment int64
	MinValue  int64
	MaxValue  int64
	Cache     int64
	Cycle     bool
}

// DDLSequenceRaw is the scan struct for the unified sequence query.
type DDLSequenceRaw struct {
	SeqParams
	Kind         string  // 'standalone', 'serial', or 'identity'
	OwnerTable   *string // quote_ident(owning table) for serial/identity
	OwnerColumn  *string // quote_ident(owning column) for serial/identity
	IdentityKind *string // 'a' or 'd' for identity sequences
	DefaultExpr  *string // nextval(...) expression for serial sequences
	OwnerRelKind *string // relkind of owning table ('r' or 'p')
}

// DDLSerialSequence represents a serial-backing sequence with its owning column.
type DDLSerialSequence struct {
	SeqParams
	TableName   string // owning table (quoted)
	ColumnName  string // owning column (quoted)
	DefaultExpr string // the nextval(...) expression
}

// DDLIdentitySequence represents an identity-backing sequence.
type DDLIdentitySequence struct {
	SeqParams
	TableName    string // owning table (quoted)
	ColumnName   string // owning column (quoted)
	IdentityKind string // 'a' (ALWAYS) or 'd' (BY DEFAULT)
	Partitioned  bool   // true if the owning table is partitioned (relkind = 'p')
}

// DDLFunction represents a function/procedure in the public schema.
type DDLFunction struct {
	OID         uint32 // p.oid (for stable ordering)
	Name        string // p.proname (for stable ordering)
	FunctionDef string // pg_get_functiondef(p.oid) — full CREATE FUNCTION text
}

// DDLTable represents a regular table or partitioned parent (relkind 'r' or 'p').
type DDLTable struct {
	OID          uint32
	Name         string  // c.relname (raw, for grouping)
	QuotedName   string  // quote_ident(c.relname)
	RelKind      string  // c.relkind: "r" or "p"
	Persistence  string  // c.relpersistence: "p" (permanent), "u" (unlogged)
	RelOptions   *string // array_to_string(c.reloptions, ', '), nil if none
	Tablespace   *string // quote_ident(spcname), nil if default
	PartitionKey *string // pg_get_partkeydef(c.oid), nil if not partitioned
	InheritsFrom *string // comma-separated quoted parent names, nil if none
}

// DDLColumn represents a single column within a table.
type DDLColumn struct {
	TableOID    uint32  // c.oid — for grouping with DDLTable
	TableName   string  // quote_ident(c.relname) — for ALTER TABLE statements
	AttNum      int16   // a.attnum — for ordering
	Name        string  // quote_ident(a.attname)
	TypeName    string  // format_type(a.atttypid, a.atttypmod)
	Collation   *string // quote_ident(collname) if non-default, nil otherwise
	Identity    *string // a.attidentity: "a" or "d", nil if not identity
	Generated   *string // a.attgenerated: "s", nil if not generated
	NotNull     bool    // a.attnotnull
	DefaultExpr *string // pg_get_expr(d.adbin, d.adrelid), nil if no default
	Storage     *string // attstorage if differs from type default, nil otherwise
	Statistics  *int32  // attstattarget if non-default (>= 0), nil otherwise
}

// DDLConstraint represents a table constraint (PK, UNIQUE, CHECK, EXCLUDE).
type DDLConstraint struct {
	TableName     string // c.relname (raw, for grouping)
	QuotedTable   string // quote_ident(c.relname)
	ConName       string // con.conname (for ordering)
	QuotedConName string // quote_ident(con.conname)
	ConType       string // con.contype: "p","u","c","x"
	ConstraintDef string // pg_get_constraintdef(con.oid)
}

// DDLPartitionChild represents a partition child table.
type DDLPartitionChild struct {
	ChildName  string // quote_ident(c.relname)
	ParentName string // quote_ident(pc.relname)
	BoundExpr  string // pg_get_expr(c.relpartbound, c.oid)
}

// DDLIndex represents an index from pg_indexes (excludes constraint-backing indexes).
type DDLIndex struct {
	IndexName string // indexname (for ordering)
	IndexDef  string // indexdef (full CREATE INDEX statement)
}

// DDLForeignKey represents a foreign key constraint emitted as ALTER TABLE.
type DDLForeignKey struct {
	TableName     string // quote_ident(c.relname)
	ConName       string // quote_ident(con.conname)
	ConstraintDef string // pg_get_constraintdef(con.oid)
}

// DDLView represents a regular view.
type DDLView struct {
	Name       string  // quote_ident(c.relname)
	ViewDef    string  // pg_get_viewdef(c.oid)
	RelOptions *string // array_to_string(c.reloptions, ', '), nil if none
}

// DDLMaterializedView represents a materialized view.
type DDLMaterializedView struct {
	Name       string  // quote_ident(c.relname)
	ViewDef    string  // pg_get_viewdef(c.oid)
	RelOptions *string // array_to_string(c.reloptions, ', '), nil if none
	Tablespace *string // quote_ident(spcname), nil if default
}

// DDLRLSEnabled represents a table with row-level security enabled.
type DDLRLSEnabled struct {
	TableName string // quote_ident(c.relname)
	Force     bool   // c.relforcerowsecurity
}

// DDLRLSPolicy represents a row-level security policy.
type DDLRLSPolicy struct {
	PolicyName string   // quote_ident(pol.polname)
	TableName  string   // quote_ident(c.relname)
	Command    string   // pol.polcmd: "r","a","w","d","*"
	Permissive bool     // pol.polpermissive
	Roles      []string // role names from polroles (empty or ["PUBLIC"] = default)
	UsingExpr  *string  // pg_get_expr(pol.polqual, pol.polrelid)
	WithCheck  *string  // pg_get_expr(pol.polwithcheck, pol.polrelid)
}

// DDLTrigger represents a user-defined trigger.
type DDLTrigger struct {
	TableName  string // c.relname (for ordering)
	TrigName   string // t.tgname (for ordering)
	TriggerDef string // pg_get_triggerdef(t.oid) — full CREATE TRIGGER text
}
