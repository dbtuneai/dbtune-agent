package queries

import (
	"fmt"
	"strings"
)

// quoteStringLiteral wraps a string in single quotes, doubling any embedded
// single quotes — equivalent to PostgreSQL's quote_literal() for simple strings.
func quoteStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func formatDDLEnumType(e *DDLEnumType) string {
	var b strings.Builder
	b.WriteString("CREATE TYPE ")
	b.WriteString(e.SchemaName)
	b.WriteByte('.')
	b.WriteString(e.TypeName)
	b.WriteString(" AS ENUM (")
	for i, label := range e.Labels {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteStringLiteral(label))
	}
	b.WriteString(");")
	return b.String()
}

func formatDDLDomainType(d *DDLDomainType) string {
	var b strings.Builder
	b.WriteString("CREATE DOMAIN ")
	b.WriteString(d.SchemaName)
	b.WriteByte('.')
	b.WriteString(d.TypeName)
	b.WriteString(" AS ")
	b.WriteString(d.BaseType)
	if d.NotNull {
		b.WriteString(" NOT NULL")
	}
	if len(d.ConstraintDefs) > 0 {
		b.WriteByte(' ')
		b.WriteString(strings.Join(d.ConstraintDefs, " "))
	}
	if d.Default != nil {
		b.WriteString(" DEFAULT ")
		b.WriteString(*d.Default)
	}
	b.WriteByte(';')
	return b.String()
}

func formatDDLCompositeType(ct *DDLCompositeType) string {
	return "CREATE TYPE " + ct.SchemaName + "." + ct.TypeName +
		" AS (\n    " + ct.Attributes + "\n);"
}

// formatCreateSequence formats a CREATE SEQUENCE statement from common params.
func formatCreateSequence(s *SeqParams) string {
	var b strings.Builder
	b.WriteString("CREATE SEQUENCE ")
	b.WriteString(s.Name)
	b.WriteString("\n    AS ")
	b.WriteString(s.DataType)
	b.WriteString("\n    START WITH ")
	b.WriteString(fmt.Sprintf("%d", s.Start))
	b.WriteString("\n    INCREMENT BY ")
	b.WriteString(fmt.Sprintf("%d", s.Increment))
	b.WriteString("\n    MINVALUE ")
	b.WriteString(fmt.Sprintf("%d", s.MinValue))
	b.WriteString("\n    MAXVALUE ")
	b.WriteString(fmt.Sprintf("%d", s.MaxValue))
	b.WriteString("\n    CACHE ")
	b.WriteString(fmt.Sprintf("%d", s.Cache))
	if s.Cycle {
		b.WriteString("\n    CYCLE")
	} else {
		b.WriteString("\n    NO CYCLE")
	}
	b.WriteByte(';')
	return b.String()
}

func formatSerialCreateSequence(s *DDLSerialSequence) string {
	return formatCreateSequence(&s.SeqParams)
}

func formatDDLFunction(f *DDLFunction) string {
	return f.FunctionDef + ";"
}

// formatDDLTable formats a bare CREATE TABLE with columns and NOT NULL only.
// Defaults for serial/identity columns are suppressed (emitted as ALTER statements).
// NOT NULL is suppressed for PK columns (PK constraint implies it).
func formatDDLTable(t *DDLTable, columns []DDLColumn, serialCols, identityCols, pkCols map[string]bool) string {
	var b strings.Builder

	if t.Persistence == "u" {
		b.WriteString("CREATE UNLOGGED TABLE ")
	} else {
		b.WriteString("CREATE TABLE ")
	}
	b.WriteString(t.QuotedName)
	b.WriteString(" (\n")

	for i, col := range columns {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteString("    ")
		b.WriteString(col.Name)
		b.WriteByte(' ')
		b.WriteString(col.TypeName)

		if col.Collation != nil {
			b.WriteString(" COLLATE ")
			b.WriteString(*col.Collation)
		}

		colKey := col.TableName + "." + col.Name
		isSerial := serialCols[colKey]
		isIdentity := identityCols[colKey]
		isGenerated := col.Generated != nil && *col.Generated == "s"

		if isGenerated {
			b.WriteString(" GENERATED ALWAYS AS (")
			if col.DefaultExpr != nil {
				b.WriteString(*col.DefaultExpr)
			}
			b.WriteString(") STORED")
		} else if !isSerial && !isIdentity && col.DefaultExpr != nil {
			b.WriteString(" DEFAULT ")
			b.WriteString(*col.DefaultExpr)
		}

		// Identity: always emit NOT NULL (required by ADD GENERATED AS IDENTITY).
		// PK: skip NOT NULL (implied by constraint, PG 18 rejects redundant).
		// Other: emit if attnotnull is true.
		isPK := pkCols[fmt.Sprintf("%d:%d", col.TableOID, col.AttNum)]
		if isIdentity || (col.NotNull && !isPK) {
			b.WriteString(" NOT NULL")
		}
	}

	b.WriteString("\n)")
	if t.InheritsFrom != nil {
		b.WriteString("\nINHERITS (")
		b.WriteString(*t.InheritsFrom)
		b.WriteByte(')')
	}
	if t.PartitionKey != nil {
		b.WriteString("\nPARTITION BY ")
		b.WriteString(*t.PartitionKey)
	}
	if t.RelOptions != nil {
		b.WriteString("\nWITH (")
		b.WriteString(*t.RelOptions)
		b.WriteByte(')')
	}
	if t.Tablespace != nil {
		b.WriteString("\nTABLESPACE ")
		b.WriteString(*t.Tablespace)
	}
	b.WriteString(";\n")
	return b.String()
}

func formatSerialDefault(s *DDLSerialSequence) string {
	return "ALTER TABLE ONLY " + s.TableName + " ALTER COLUMN " + s.ColumnName +
		" SET DEFAULT " + s.DefaultExpr + ";"
}

func formatSequenceOwnedBy(s *DDLSerialSequence) string {
	return "ALTER SEQUENCE " + s.Name + " OWNED BY " + s.TableName + "." + s.ColumnName + ";"
}

func formatIdentityColumn(s *DDLIdentitySequence) string {
	var b strings.Builder
	if s.Partitioned {
		b.WriteString("ALTER TABLE ")
	} else {
		b.WriteString("ALTER TABLE ONLY ")
	}
	b.WriteString(s.TableName)
	b.WriteString(" ALTER COLUMN ")
	b.WriteString(s.ColumnName)
	if s.IdentityKind == "a" {
		b.WriteString(" ADD GENERATED ALWAYS AS IDENTITY (\n")
	} else {
		b.WriteString(" ADD GENERATED BY DEFAULT AS IDENTITY (\n")
	}
	b.WriteString("    SEQUENCE NAME ")
	b.WriteString(s.Name)
	b.WriteString("\n    START WITH ")
	b.WriteString(fmt.Sprintf("%d", s.Start))
	b.WriteString("\n    INCREMENT BY ")
	b.WriteString(fmt.Sprintf("%d", s.Increment))
	b.WriteString("\n    MINVALUE ")
	b.WriteString(fmt.Sprintf("%d", s.MinValue))
	b.WriteString("\n    MAXVALUE ")
	b.WriteString(fmt.Sprintf("%d", s.MaxValue))
	b.WriteString("\n    CACHE ")
	b.WriteString(fmt.Sprintf("%d", s.Cache))
	if s.Cycle {
		b.WriteString("\n    CYCLE")
	} else {
		b.WriteString("\n    NO CYCLE")
	}
	b.WriteString("\n);")
	return b.String()
}

var storageNames = map[string]string{
	"p": "PLAIN", "e": "EXTERNAL", "m": "MAIN", "x": "EXTENDED",
}

func formatColumnSetStorage(col *DDLColumn) string {
	return "ALTER TABLE ONLY " + col.TableName + " ALTER COLUMN " + col.Name +
		" SET STORAGE " + storageNames[*col.Storage] + ";"
}

func formatColumnSetStatistics(col *DDLColumn) string {
	return fmt.Sprintf("ALTER TABLE ONLY %s ALTER COLUMN %s SET STATISTICS %d;",
		col.TableName, col.Name, *col.Statistics)
}

func formatDDLConstraint(c *DDLConstraint) string {
	return "ALTER TABLE ONLY " + c.QuotedTable + "\n    ADD CONSTRAINT " +
		c.QuotedConName + " " + c.ConstraintDef + ";"
}

func formatDDLPartitionChild(p *DDLPartitionChild) string {
	return "CREATE TABLE " + p.ChildName + " PARTITION OF " + p.ParentName +
		" " + p.BoundExpr + ";"
}

func formatDDLIndex(idx *DDLIndex) string {
	return idx.IndexDef + ";"
}

func formatDDLForeignKey(fk *DDLForeignKey) string {
	return "ALTER TABLE ONLY " + fk.TableName + "\n    ADD CONSTRAINT " +
		fk.ConName + " " + fk.ConstraintDef + ";"
}

func formatDDLView(v *DDLView) string {
	var b strings.Builder
	b.WriteString("CREATE VIEW ")
	b.WriteString(v.Name)
	if v.RelOptions != nil {
		b.WriteString(" WITH (")
		b.WriteString(*v.RelOptions)
		b.WriteByte(')')
	}
	b.WriteString(" AS\n")
	b.WriteString(strings.TrimRight(v.ViewDef, "; \n\t"))
	b.WriteByte(';')
	return b.String()
}

func formatDDLMaterializedView(mv *DDLMaterializedView) string {
	var b strings.Builder
	b.WriteString("CREATE MATERIALIZED VIEW ")
	b.WriteString(mv.Name)
	if mv.RelOptions != nil {
		b.WriteString("\nWITH (")
		b.WriteString(*mv.RelOptions)
		b.WriteByte(')')
	}
	if mv.Tablespace != nil {
		b.WriteString("\nTABLESPACE ")
		b.WriteString(*mv.Tablespace)
	}
	b.WriteString(" AS\n")
	b.WriteString(strings.TrimRight(mv.ViewDef, "; \n\t"))
	b.WriteString("\nWITH NO DATA;")
	return b.String()
}

func formatDDLRLSEnable(r *DDLRLSEnabled) string {
	s := "ALTER TABLE " + r.TableName + " ENABLE ROW LEVEL SECURITY;"
	if r.Force {
		s += "\nALTER TABLE " + r.TableName + " FORCE ROW LEVEL SECURITY;"
	}
	return s
}

func formatDDLRLSPolicy(p *DDLRLSPolicy) string {
	var b strings.Builder
	b.WriteString("CREATE POLICY ")
	b.WriteString(p.PolicyName)
	b.WriteString(" ON ")
	b.WriteString(p.TableName)

	switch p.Command {
	case "r":
		b.WriteString(" FOR SELECT")
	case "a":
		b.WriteString(" FOR INSERT")
	case "w":
		b.WriteString(" FOR UPDATE")
	case "d":
		b.WriteString(" FOR DELETE")
	}
	if !p.Permissive {
		b.WriteString(" AS RESTRICTIVE")
	}
	if len(p.Roles) > 0 && !(len(p.Roles) == 1 && p.Roles[0] == "PUBLIC") {
		b.WriteString(" TO ")
		b.WriteString(strings.Join(p.Roles, ", "))
	}
	if p.UsingExpr != nil {
		b.WriteString("\n  USING (")
		b.WriteString(*p.UsingExpr)
		b.WriteByte(')')
	}
	if p.WithCheck != nil {
		b.WriteString("\n  WITH CHECK (")
		b.WriteString(*p.WithCheck)
		b.WriteByte(')')
	}
	b.WriteByte(';')
	return b.String()
}

func formatDDLTrigger(tr *DDLTrigger) string {
	return tr.TriggerDef + ";"
}
