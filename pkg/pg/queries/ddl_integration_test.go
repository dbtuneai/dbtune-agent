//go:build integration

package queries

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func collectDDLString(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	ddl, err := CollectDDL(pool, ctx)
	if err != nil {
		t.Fatalf("CollectDDL() error: %v", err)
	}
	return ddl
}

func execDDL(t *testing.T, pool *pgxpool.Pool, createSQL, tableName string) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, createSQL); err != nil {
		t.Fatalf("setup %q: %v", tableName, err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+tableName+" CASCADE")
	})
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestDDL_ColumnTypes(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		tests := []struct {
			name     string
			sqlType  string
			expected string
		}{
			{"integer", "integer", "integer"},
			{"bigint", "bigint", "bigint"},
			{"smallint", "smallint", "smallint"},
			{"text", "text", "text"},
			{"boolean", "boolean", "boolean"},
			{"real", "real", "real"},
			{"double_precision", "double precision", "double precision"},
			{"varchar_100", "varchar(100)", "character varying(100)"},
			{"char_10", "char(10)", "character(10)"},
			{"numeric_10_2", "numeric(10,2)", "numeric(10,2)"},
			{"timestamp", "timestamp", "timestamp without time zone"},
			{"timestamptz", "timestamptz", "timestamp with time zone"},
			{"timestamp_3", "timestamp(3)", "timestamp(3) without time zone"},
			{"date", "date", "date"},
			{"time", "time", "time without time zone"},
			{"interval", "interval", "interval"},
			{"uuid", "uuid", "uuid"},
			{"json", "json", "json"},
			{"jsonb", "jsonb", "jsonb"},
			{"bytea", "bytea", "bytea"},
			{"integer_array", "integer[]", "integer[]"},
			{"text_array", "text[]", "text[]"},
			{"inet", "inet", "inet"},
			{"cidr", "cidr", "cidr"},
			{"macaddr", "macaddr", "macaddr"},
			{"tsvector", "tsvector", "tsvector"},
			{"xml", "xml", "xml"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				table := "ddl_coltype_" + tt.name
				execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (val %s)", table, tt.sqlType), table)
				ddl := collectDDLString(t, inst.pool)
				if !strings.Contains(ddl, tt.expected) {
					t.Fatalf("expected DDL to contain %q for type %s, got:\n%s", tt.expected, tt.sqlType, ddl)
				}
			})
		}
	})
}

func TestDDL_Defaults(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		tests := []struct {
			name     string
			colDef   string
			expected string
		}{
			{"integer_default", "x integer DEFAULT 42", "DEFAULT 42"},
			{"string_default", "x text DEFAULT 'hello'", "DEFAULT 'hello'"},
			{"boolean_default", "x boolean DEFAULT true", "DEFAULT true"},
			{"now_default", "x timestamptz DEFAULT now()", "DEFAULT now()"},
			{"expression_default", "x integer DEFAULT 1 + 1", "DEFAULT"},
			{"gen_random_uuid", "x uuid DEFAULT gen_random_uuid()", "DEFAULT gen_random_uuid()"},
			{"serial", "x serial", "nextval("},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				table := "ddl_default_" + tt.name
				execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (%s)", table, tt.colDef), table)
				ddl := collectDDLString(t, inst.pool)

				// Search the full DDL for the expected pattern (some defaults
				// like serial are emitted as ALTER TABLE ... SET DEFAULT).
				if !strings.Contains(ddl, tt.expected) {
					t.Fatalf("expected DDL to contain %q, got:\n%s", tt.expected, ddl)
				}
			})
		}

		t.Run("no_default", func(t *testing.T) {
			table := "ddl_default_none"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (plain_col integer)", table), table)
			ddl := collectDDLString(t, inst.pool)

			tableIdx := strings.Index(ddl, table)
			if tableIdx < 0 {
				t.Fatalf("table %s not found in DDL", table)
			}
			tableDDL := ddl[tableIdx:]
			endIdx := strings.Index(tableDDL, ");")
			if endIdx > 0 {
				tableDDL = tableDDL[:endIdx]
			}
			for _, line := range strings.Split(tableDDL, "\n") {
				if strings.Contains(line, "plain_col") && strings.Contains(line, "DEFAULT") {
					t.Fatalf("expected no DEFAULT for plain_col, got: %s", line)
				}
			}
		})
	})
}

func TestDDL_NotNull(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		table := "ddl_notnull"
		execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s (
			required integer NOT NULL,
			optional integer
		)`, table), table)

		ddl := collectDDLString(t, inst.pool)
		tableIdx := strings.Index(ddl, table)
		if tableIdx < 0 {
			t.Fatalf("table %s not found in DDL", table)
		}
		tableDDL := ddl[tableIdx:]
		endIdx := strings.Index(tableDDL, ");")
		if endIdx > 0 {
			tableDDL = tableDDL[:endIdx]
		}
		for _, line := range strings.Split(tableDDL, "\n") {
			if strings.Contains(line, "required") && !strings.Contains(line, "NOT NULL") {
				t.Fatalf("expected NOT NULL for 'required', got: %s", line)
			}
			if strings.Contains(line, "optional") && strings.Contains(line, "NOT NULL") {
				t.Fatalf("expected no NOT NULL for 'optional', got: %s", line)
			}
		}
	})
}

func TestDDL_Constraints(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		t.Run("single_pk", func(t *testing.T) {
			table := "ddl_con_single_pk"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, PRIMARY KEY (id))", table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "PRIMARY KEY (id)") {
				t.Fatalf("expected PRIMARY KEY (id) in DDL:\n%s", ddl)
			}
		})

		t.Run("composite_pk", func(t *testing.T) {
			table := "ddl_con_comp_pk"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (a integer, b integer, PRIMARY KEY (a, b))", table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "PRIMARY KEY (a, b)") {
				t.Fatalf("expected PRIMARY KEY (a, b) in DDL:\n%s", ddl)
			}
		})

		t.Run("single_unique", func(t *testing.T) {
			table := "ddl_con_unique"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, email text, UNIQUE (email))", table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "UNIQUE (email)") {
				t.Fatalf("expected UNIQUE (email) in DDL:\n%s", ddl)
			}
		})

		t.Run("no_constraints", func(t *testing.T) {
			table := "ddl_con_none"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (a integer, b text)", table), table)
			ddl := collectDDLString(t, inst.pool)
			tableIdx := strings.Index(ddl, table)
			if tableIdx < 0 {
				t.Fatalf("table %s not found in DDL", table)
			}
			tableDDL := ddl[tableIdx:]
			endIdx := strings.Index(tableDDL, ");")
			if endIdx > 0 {
				tableDDL = tableDDL[:endIdx]
			}
			if strings.Contains(tableDDL, "PRIMARY KEY") || strings.Contains(tableDDL, "UNIQUE") {
				t.Fatalf("expected no constraints for %s, got:\n%s", table, tableDDL)
			}
		})
	})
}

func TestDDL_Indexes(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		baseTable := "ddl_idx_base"
		execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s (
			id integer, name text, score integer, active boolean,
			tags text[], created_at timestamptz DEFAULT now(), extra text
		)`, baseTable), baseTable)

		tests := []struct {
			name, indexSQL, expected, indexName string
		}{
			{"simple_btree", "CREATE INDEX ddl_idx_btree ON ddl_idx_base (name)", "ddl_idx_btree", "ddl_idx_btree"},
			{"unique_index", "CREATE UNIQUE INDEX ddl_idx_uniq ON ddl_idx_base (id)", "CREATE UNIQUE INDEX", "ddl_idx_uniq"},
			{"multi_column", "CREATE INDEX ddl_idx_multi ON ddl_idx_base (name, score)", "name, score", "ddl_idx_multi"},
			{"partial_index", "CREATE INDEX ddl_idx_partial ON ddl_idx_base (name) WHERE active", "WHERE", "ddl_idx_partial"},
			{"expression_index", "CREATE INDEX ddl_idx_expr ON ddl_idx_base (lower(name))", "lower(", "ddl_idx_expr"},
			{"desc_sort", "CREATE INDEX ddl_idx_desc ON ddl_idx_base (score DESC)", "DESC", "ddl_idx_desc"},
			{"gin_index", "CREATE INDEX ddl_idx_gin ON ddl_idx_base USING gin (tags)", "USING gin", "ddl_idx_gin"},
			{"brin_index", "CREATE INDEX ddl_idx_brin ON ddl_idx_base USING brin (created_at)", "USING brin", "ddl_idx_brin"},
			{"hash_index", "CREATE INDEX ddl_idx_hash ON ddl_idx_base USING hash (name)", "USING hash", "ddl_idx_hash"},
			{"include_columns", "CREATE INDEX ddl_idx_incl ON ddl_idx_base (name) INCLUDE (extra)", "INCLUDE", "ddl_idx_incl"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if _, err := inst.admin.Exec(ctx, tt.indexSQL); err != nil {
					t.Fatalf("create index: %v", err)
				}
				t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, "DROP INDEX IF EXISTS "+tt.indexName) })

				ddl := collectDDLString(t, inst.pool)
				if !strings.Contains(ddl, tt.expected) {
					t.Fatalf("expected %q in DDL:\n%s", tt.expected, ddl)
				}
			})
		}
	})
}

func TestDDL_EdgeCases(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("dropped_column_excluded", func(t *testing.T) {
			table := "ddl_edge_dropped"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (keep_me integer, drop_me text)", table), table)
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN drop_me", table))
			ddl := collectDDLString(t, inst.pool)
			if strings.Contains(ddl, "drop_me") {
				t.Fatalf("dropped column should not appear in DDL:\n%s", ddl)
			}
		})

		t.Run("table_ordering_alphabetical", func(t *testing.T) {
			tableZ := "ddl_edge_z_table"
			tableA := "ddl_edge_a_table"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (val integer)", tableZ), tableZ)
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (val integer)", tableA), tableA)
			ddl := collectDDLString(t, inst.pool)
			if strings.Index(ddl, tableA) > strings.Index(ddl, tableZ) {
				t.Fatalf("expected %s before %s (alphabetical)", tableA, tableZ)
			}
		})

		t.Run("reserved_word_column", func(t *testing.T) {
			table := "ddl_edge_quoted"
			execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s ("order" integer, "select" text)`, table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, `"order"`) || !strings.Contains(ddl, `"select"`) {
				t.Fatalf("expected quoted reserved-word columns in DDL:\n%s", ddl)
			}
		})

		t.Run("many_columns", func(t *testing.T) {
			table := "ddl_edge_manycols"
			var cols []string
			for i := 0; i < 25; i++ {
				cols = append(cols, fmt.Sprintf("col_%02d integer", i))
			}
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (%s)", table, strings.Join(cols, ", ")), table)
			ddl := collectDDLString(t, inst.pool)
			for i := 0; i < 25; i++ {
				if !strings.Contains(ddl, fmt.Sprintf("col_%02d", i)) {
					t.Fatalf("missing col_%02d in DDL", i)
				}
			}
		})
	})
}

func TestDDL_ScopeExclusions(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("non_public_schema_included", func(t *testing.T) {
			schema := "ddl_other_schema"
			table := "ddl_scope_visible"
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s (val integer)", schema, table))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)) })
			ddl := collectDDLString(t, inst.pool)
			qualifiedName := schema + "." + table
			if !strings.Contains(ddl, qualifiedName) {
				t.Fatalf("non-public schema table should appear as %s in DDL:\n%s", qualifiedName, ddl)
			}
		})

		t.Run("system_schemas_excluded", func(t *testing.T) {
			ddl := collectDDLString(t, inst.pool)
			if strings.Contains(ddl, "pg_catalog.") || strings.Contains(ddl, "information_schema.") {
				t.Fatalf("system schema objects should not appear in DDL:\n%s", ddl)
			}
		})
	})
}

func TestDDL_IdentifierQuoting(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		t.Run("mixed_case_column", func(t *testing.T) {
			table := "ddl_quote_mixcase"
			execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s ("userId" integer, "createdAt" timestamptz)`, table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, `"userId"`) || !strings.Contains(ddl, `"createdAt"`) {
				t.Fatalf("expected quoted mixed-case columns in DDL:\n%s", ddl)
			}
		})

		t.Run("reserved_word_table", func(t *testing.T) {
			execDDL(t, inst.admin, `CREATE TABLE "user" (id integer)`, `"user"`)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, `CREATE TABLE public."user"`) {
				t.Fatalf("expected 'CREATE TABLE public.\"user\"' in DDL:\n%s", ddl)
			}
		})
	})
}

func TestDDL_ForeignKeys(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		t.Run("simple_fk", func(t *testing.T) {
			parent := "ddl_fk_parent"
			child := "ddl_fk_child"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer PRIMARY KEY)", parent), parent)
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, parent_id integer REFERENCES %s(id))", child, parent), child)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "FOREIGN KEY") || !strings.Contains(ddl, "REFERENCES") {
				t.Fatalf("expected FOREIGN KEY ... REFERENCES in DDL:\n%s", ddl)
			}
		})

		t.Run("self_referencing_fk", func(t *testing.T) {
			table := "ddl_fk_self"
			execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s (id integer PRIMARY KEY, parent_id integer REFERENCES %s(id))`, table, table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "REFERENCES") {
				t.Fatalf("expected self-referencing FK in DDL:\n%s", ddl)
			}
		})
	})
}

func TestDDL_ForeignKeysAsAlterTable(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("fk_emitted_after_all_tables", func(t *testing.T) {
			parent := "ddl_fkorder_z_parent"
			child := "ddl_fkorder_a_child"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer PRIMARY KEY)", parent), parent)
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, parent_id integer REFERENCES %s(id))", child, parent), child)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "ALTER TABLE") {
				t.Fatalf("expected FK as ALTER TABLE:\n%s", ddl)
			}
			lastCreate := strings.LastIndex(ddl, "CREATE TABLE")
			firstAlter := strings.Index(ddl, "ALTER TABLE")
			if firstAlter < lastCreate {
				t.Fatalf("ALTER TABLE FK appears before last CREATE TABLE:\n%s", ddl)
			}
		})

		t.Run("circular_fk", func(t *testing.T) {
			tblA := "ddl_fkcirc_a"
			tblB := "ddl_fkcirc_b"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer PRIMARY KEY, b_id integer)", tblA), tblA)
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer PRIMARY KEY, a_id integer REFERENCES %s(id))", tblB, tblA), tblB)
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT fk_to_b FOREIGN KEY (b_id) REFERENCES %s(id)", tblA, tblB))
			ddl := collectDDLString(t, inst.pool)
			if strings.Count(ddl, "ALTER TABLE") < 2 {
				t.Fatalf("expected at least 2 ALTER TABLE FK for circular refs:\n%s", ddl)
			}
		})
	})
}

func TestDDL_CheckConstraints(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		t.Run("simple_check", func(t *testing.T) {
			table := "ddl_chk_simple"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (x integer CHECK (x > 0))", table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "CHECK") || !strings.Contains(ddl, "x > 0") {
				t.Fatalf("expected CHECK (x > 0) in DDL:\n%s", ddl)
			}
		})

		t.Run("multiple_checks", func(t *testing.T) {
			table := "ddl_chk_mult"
			execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s (x integer CHECK (x > 0), y integer CHECK (y > 0))`, table), table)
			ddl := collectDDLString(t, inst.pool)
			if strings.Count(ddl, "CHECK") < 2 {
				t.Fatalf("expected at least 2 CHECK constraints:\n%s", ddl)
			}
		})
	})
}

func TestDDL_Sequences(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("serial_sequence", func(t *testing.T) {
			table := "ddl_seq_serial"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id serial PRIMARY KEY, name text)", table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "CREATE SEQUENCE") {
				t.Fatalf("expected CREATE SEQUENCE for serial in DDL:\n%s", ddl)
			}
		})

		t.Run("standalone_sequence", func(t *testing.T) {
			seqName := "ddl_seq_standalone"
			qualifiedSeqName := "public." + seqName
			table := "ddl_seq_standalone_tbl"
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE SEQUENCE %s", seqName))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, "DROP SEQUENCE IF EXISTS "+seqName+" CASCADE") })
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer DEFAULT nextval('%s'))", table, seqName), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "CREATE SEQUENCE "+qualifiedSeqName) {
				t.Fatalf("expected CREATE SEQUENCE %s in DDL:\n%s", qualifiedSeqName, ddl)
			}
		})
	})
}

func TestDDL_EnumTypes(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("simple_enum", func(t *testing.T) {
			typeName := "ddl_status_enum"
			table := "ddl_enum_simple"
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TYPE %s AS ENUM ('active', 'inactive', 'pending')", typeName))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, "DROP TYPE IF EXISTS "+typeName+" CASCADE") })
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, status %s)", table, typeName), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "CREATE TYPE") || !strings.Contains(ddl, "'active'") {
				t.Fatalf("expected CREATE TYPE with enum values in DDL:\n%s", ddl)
			}
		})
	})
}

func TestDDL_DomainTypes(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("simple_domain", func(t *testing.T) {
			domainName := "ddl_email_domain"
			table := "ddl_domain_simple"
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE DOMAIN %s AS text CHECK (VALUE ~ '@')", domainName))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, "DROP DOMAIN IF EXISTS "+domainName+" CASCADE") })
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, email %s)", table, domainName), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "CREATE DOMAIN") || !strings.Contains(ddl, domainName) {
				t.Fatalf("expected CREATE DOMAIN in DDL:\n%s", ddl)
			}
		})
	})
}

func TestDDL_ExclusionConstraints(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		_, _ = inst.admin.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS btree_gist")
		t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, "DROP EXTENSION IF EXISTS btree_gist CASCADE") })

		table := "ddl_excl_simple"
		execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s (room integer, during tstzrange, EXCLUDE USING gist (room WITH =, during WITH &&))`, table), table)
		ddl := collectDDLString(t, inst.pool)
		if !strings.Contains(ddl, "EXCLUDE") {
			t.Fatalf("expected EXCLUDE in DDL:\n%s", ddl)
		}
	})
}

func TestDDL_GeneratedColumns(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		table := "ddl_gen_stored"
		execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s (a integer, b integer, total integer GENERATED ALWAYS AS (a + b) STORED)`, table), table)
		ddl := collectDDLString(t, inst.pool)
		if !strings.Contains(ddl, "GENERATED ALWAYS AS") {
			t.Fatalf("expected GENERATED ALWAYS AS in DDL:\n%s", ddl)
		}
	})
}

func TestDDL_IdentityColumns(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		t.Run("always_identity", func(t *testing.T) {
			table := "ddl_ident_always"
			execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s (id integer GENERATED ALWAYS AS IDENTITY, name text)`, table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "GENERATED") {
				t.Fatalf("expected GENERATED ... AS IDENTITY in DDL:\n%s", ddl)
			}
		})

		t.Run("by_default_identity", func(t *testing.T) {
			table := "ddl_ident_bydefault"
			execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s (id integer GENERATED BY DEFAULT AS IDENTITY, name text)`, table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "GENERATED") {
				t.Fatalf("expected GENERATED BY DEFAULT AS IDENTITY in DDL:\n%s", ddl)
			}
		})
	})
}

func TestDDL_Partitioning(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("range_partition", func(t *testing.T) {
			parent := "ddl_part_range"
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id integer, created_at date) PARTITION BY RANGE (created_at)", parent))
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TABLE ddl_part_range_2023 PARTITION OF %s FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')", parent))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", parent)) })
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "PARTITION BY RANGE") || !strings.Contains(ddl, "FOR VALUES FROM") {
				t.Fatalf("expected PARTITION BY RANGE + FOR VALUES FROM in DDL:\n%s", ddl)
			}
		})

		t.Run("list_partition", func(t *testing.T) {
			parent := "ddl_part_list"
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id integer, status text) PARTITION BY LIST (status)", parent))
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TABLE ddl_part_list_active PARTITION OF %s FOR VALUES IN ('active')", parent))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", parent)) })
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "PARTITION BY LIST") || !strings.Contains(ddl, "FOR VALUES IN") {
				t.Fatalf("expected PARTITION BY LIST + FOR VALUES IN in DDL:\n%s", ddl)
			}
		})
	})
}

func TestDDL_Inheritance(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		parent := "ddl_inherit_parent"
		child := "ddl_inherit_child"
		execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, name text)", parent), parent)
		_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (extra integer) INHERITS (%s)", child, parent))
		t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", child)) })
		ddl := collectDDLString(t, inst.pool)
		if !strings.Contains(ddl, "INHERITS") {
			t.Fatalf("expected INHERITS in DDL:\n%s", ddl)
		}
	})
}

func TestDDL_RLSPolicies(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		table := "ddl_rls_simple"
		execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, owner_id integer)", table), table)
		_, _ = inst.admin.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", table))
		_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE POLICY owner_only ON %s USING (owner_id = current_setting('app.user_id')::integer)", table))
		ddl := collectDDLString(t, inst.pool)
		if !strings.Contains(ddl, "ENABLE ROW LEVEL SECURITY") {
			t.Fatalf("expected ENABLE ROW LEVEL SECURITY in DDL:\n%s", ddl)
		}
		if !strings.Contains(ddl, "CREATE POLICY") {
			t.Fatalf("expected CREATE POLICY in DDL:\n%s", ddl)
		}
	})
}

func TestDDL_Triggers(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		funcName := "ddl_trig_func"
		_, _ = inst.admin.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger AS $$ BEGIN NEW.id = COALESCE(NEW.id, 0); RETURN NEW; END; $$ LANGUAGE plpgsql`, funcName))
		t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s() CASCADE", funcName)) })

		table := "ddl_trig_simple"
		execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer)", table), table)
		_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TRIGGER trg_before_insert BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s()", table, funcName))
		ddl := collectDDLString(t, inst.pool)
		if !strings.Contains(ddl, "CREATE TRIGGER") || !strings.Contains(ddl, "trg_before_insert") {
			t.Fatalf("expected CREATE TRIGGER trg_before_insert in DDL:\n%s", ddl)
		}
	})
}

func TestDDL_Views(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("simple_view", func(t *testing.T) {
			table := "ddl_view_base"
			view := "ddl_view_simple"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, name text, active boolean)", table), table)
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE VIEW %s AS SELECT id, name FROM %s WHERE active", view, table))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s", view)) })
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "CREATE VIEW") || !strings.Contains(ddl, view) {
				t.Fatalf("expected CREATE VIEW %s in DDL:\n%s", view, ddl)
			}
		})

		t.Run("materialized_view", func(t *testing.T) {
			table := "ddl_matview_base"
			matview := "ddl_matview_simple"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, score integer)", table), table)
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS SELECT count(*) AS cnt FROM %s", matview, table))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s", matview)) })
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "CREATE MATERIALIZED VIEW") || !strings.Contains(ddl, matview) {
				t.Fatalf("expected CREATE MATERIALIZED VIEW %s in DDL:\n%s", matview, ddl)
			}
		})
	})
}

func TestDDL_Functions(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("plpgsql_function", func(t *testing.T) {
			funcName := "ddl_func_add"
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION %s(a integer, b integer) RETURNS integer AS $$ BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql`, funcName))
			t.Cleanup(func() {
				_, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s(integer, integer)", funcName))
			})
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "FUNCTION") || !strings.Contains(ddl, funcName) {
				t.Fatalf("expected function %s in DDL:\n%s", funcName, ddl)
			}
		})
	})
}

func TestDDL_StorageParameters(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		t.Run("table_fillfactor", func(t *testing.T) {
			table := "ddl_storage_fill"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer) WITH (fillfactor=70)", table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "fillfactor") {
				t.Fatalf("expected fillfactor in DDL:\n%s", ddl)
			}
		})

		t.Run("table_autovacuum_params", func(t *testing.T) {
			table := "ddl_storage_autovac"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer) WITH (autovacuum_vacuum_scale_factor=0.01)", table), table)
			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, "autovacuum_vacuum_scale_factor") {
				t.Fatalf("expected autovacuum params in DDL:\n%s", ddl)
			}
		})
	})
}

func TestDDL_ColumnCollation(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		table := "ddl_collation"
		execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s (name text COLLATE "C", normal text)`, table), table)
		ddl := collectDDLString(t, inst.pool)
		tableIdx := strings.Index(ddl, table)
		if tableIdx < 0 {
			t.Fatalf("table %s not found in DDL", table)
		}
		tableDDL := ddl[tableIdx:]
		if endIdx := strings.Index(tableDDL, ");"); endIdx > 0 {
			tableDDL = tableDDL[:endIdx]
		}
		if !strings.Contains(tableDDL, "COLLATE") {
			t.Fatalf("expected COLLATE for non-default collation column:\n%s", tableDDL)
		}
		for _, line := range strings.Split(tableDDL, "\n") {
			if strings.Contains(line, "normal") && strings.Contains(line, "COLLATE") {
				t.Fatalf("expected no COLLATE for default-collation column:\n%s", line)
			}
		}
	})
}

func TestDDL_CrossSchemaTypes(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		schema := "ddl_types_schema"
		typeName := "ext_status"
		table := "ddl_cross_enum"
		_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
		_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TYPE %s.%s AS ENUM ('on', 'off')", schema, typeName))
		t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)) })
		execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, status %s.%s)", table, schema, typeName), table)
		ddl := collectDDLString(t, inst.pool)
		if !strings.Contains(ddl, "CREATE TYPE") || !strings.Contains(ddl, typeName) {
			t.Fatalf("expected CREATE TYPE for cross-schema enum in DDL:\n%s", ddl)
		}
	})
}

// TestDDL_NonPublicSchema verifies that objects in non-public schemas are
// correctly collected with schema-qualified names, and that the output
// can be replayed and matches pg_dump.
func TestDDL_NonPublicSchema(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("table_in_custom_schema", func(t *testing.T) {
			schema := "app_data"
			table := "customers"
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s (id integer PRIMARY KEY, name text NOT NULL)", schema, table))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)) })

			ddl := collectDDLString(t, inst.pool)

			// Must contain the schema-qualified table name.
			qualifiedTable := schema + "." + table
			if !strings.Contains(ddl, "CREATE TABLE "+qualifiedTable) {
				t.Fatalf("expected CREATE TABLE %s in DDL:\n%s", qualifiedTable, ddl)
			}
			// Must contain the CREATE SCHEMA statement.
			if !strings.Contains(ddl, "CREATE SCHEMA IF NOT EXISTS "+schema) {
				t.Fatalf("expected CREATE SCHEMA %s in DDL:\n%s", schema, ddl)
			}
			// The constraint should also be schema-qualified.
			if !strings.Contains(ddl, "ALTER TABLE ONLY "+qualifiedTable) {
				t.Fatalf("expected ALTER TABLE ONLY %s for PK in DDL:\n%s", qualifiedTable, ddl)
			}
		})

		t.Run("fk_across_schemas", func(t *testing.T) {
			schema := "orders_schema"
			parentTable := "ddl_ns_parent"
			childTable := "ddl_ns_child"
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s (id integer PRIMARY KEY)", schema, parentTable))
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf(
				"CREATE TABLE %s.%s (id integer PRIMARY KEY, parent_id integer REFERENCES %s.%s(id))",
				schema, childTable, schema, parentTable,
			))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)) })

			ddl := collectDDLString(t, inst.pool)

			qualifiedChild := schema + "." + childTable
			if !strings.Contains(ddl, "ALTER TABLE ONLY "+qualifiedChild) || !strings.Contains(ddl, "FOREIGN KEY") {
				t.Fatalf("expected schema-qualified FK constraint for %s in DDL:\n%s", qualifiedChild, ddl)
			}
		})

		t.Run("index_in_custom_schema", func(t *testing.T) {
			schema := "idx_schema"
			table := "ddl_ns_indexed"
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s (id integer, val text)", schema, table))
			_, _ = inst.admin.Exec(ctx, fmt.Sprintf("CREATE INDEX idx_ns_val ON %s.%s (val)", schema, table))
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)) })

			ddl := collectDDLString(t, inst.pool)
			// pg_indexes.indexdef is schema-qualified by PostgreSQL itself.
			qualifiedTable := schema + "." + table
			if !strings.Contains(ddl, qualifiedTable) || !strings.Contains(ddl, "idx_ns_val") {
				t.Fatalf("expected schema-qualified index on %s in DDL:\n%s", qualifiedTable, ddl)
			}
		})

		t.Run("roundtrip_non_public_schema", func(t *testing.T) {
			// Create a fresh database with objects in a non-public schema,
			// collect DDL, replay it, and verify it reproduces the same schema.
			sourceDB := "ddl_ns_source"
			replayDB := "ddl_ns_replay"

			_, _ = inst.admin.Exec(ctx, "DROP DATABASE IF EXISTS "+sourceDB)
			_, err := inst.admin.Exec(ctx, "CREATE DATABASE "+sourceDB)
			if err != nil {
				t.Fatalf("CREATE DATABASE %s failed: %v", sourceDB, err)
			}
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, "DROP DATABASE IF EXISTS "+sourceDB) })

			sourceConnStr := replaceDBInConnStr(inst.admin, sourceDB)
			sourceAdmin, err := pgxpool.New(ctx, sourceConnStr)
			if err != nil {
				t.Fatalf("connect to source: %v", err)
			}
			defer sourceAdmin.Close()

			// Create schema with tables, FK, and index.
			for _, sql := range []string{
				"CREATE SCHEMA myapp",
				"CREATE TABLE myapp.users (id integer PRIMARY KEY, name text NOT NULL)",
				"CREATE TABLE myapp.orders (id integer PRIMARY KEY, user_id integer REFERENCES myapp.users(id), total numeric(10,2))",
				"CREATE INDEX idx_orders_user ON myapp.orders (user_id)",
			} {
				if _, err := sourceAdmin.Exec(ctx, sql); err != nil {
					t.Fatalf("setup: %v\nSQL: %s", err, sql)
				}
			}

			// Create monitor role.
			for _, sql := range []string{
				`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'monitor') THEN CREATE ROLE monitor LOGIN PASSWORD 'monitor'; END IF; END $$`,
				`GRANT pg_monitor TO monitor`,
			} {
				if _, err := sourceAdmin.Exec(ctx, sql); err != nil {
					t.Fatalf("monitor setup: %v", err)
				}
			}

			monitorConnStr := strings.Replace(sourceConnStr, "user=test", "user=monitor", 1)
			monitorConnStr = strings.Replace(monitorConnStr, "password=test", "password=monitor", 1)
			sourceMonitor, err := pgxpool.New(ctx, monitorConnStr)
			if err != nil {
				t.Fatalf("connect as monitor: %v", err)
			}
			defer sourceMonitor.Close()

			ourDDL, err := CollectDDL(sourceMonitor, ctx)
			if err != nil {
				t.Fatalf("CollectDDL: %v", err)
			}

			// Replay into fresh DB.
			_, _ = inst.admin.Exec(ctx, "DROP DATABASE IF EXISTS "+replayDB)
			_, err = inst.admin.Exec(ctx, "CREATE DATABASE "+replayDB)
			if err != nil {
				t.Fatalf("CREATE DATABASE %s failed: %v", replayDB, err)
			}
			t.Cleanup(func() { _, _ = inst.admin.Exec(ctx, "DROP DATABASE IF EXISTS "+replayDB) })

			replayConnStr := replaceDBInConnStr(inst.admin, replayDB)
			replayPool, err := pgxpool.New(ctx, replayConnStr)
			if err != nil {
				t.Fatalf("connect to replay: %v", err)
			}
			defer replayPool.Close()

			if _, err := replayPool.Exec(ctx, ourDDL); err != nil {
				t.Fatalf("replay failed:\nerror: %v\nDDL:\n%s", err, ourDDL)
			}

			// Compare pg_dump output between source and replay.
			sourceDump := pgDumpSchemaOnly(t, inst, sourceDB)
			replayDump := pgDumpSchemaOnly(t, inst, replayDB)

			if sourceDump != replayDump {
				t.Errorf("pg_dump output differs:\n%s", unifiedTextDiff(sourceDump, replayDump))
			}
		})
	})
}

// TestDDL_HashChangesOnAlter verifies that altering a table produces a different
// DDL snapshot and a different hash.
func TestDDL_HashChangesOnAlter(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		t.Run("add_column", func(t *testing.T) {
			table := "ddl_hash_add_col"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer PRIMARY KEY)", table), table)

			ddl1 := collectDDLString(t, inst.pool)
			hash1 := HashDDL(ddl1)

			if _, err := inst.admin.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN name text", table)); err != nil {
				t.Fatalf("ALTER TABLE: %v", err)
			}

			ddl2 := collectDDLString(t, inst.pool)
			hash2 := HashDDL(ddl2)

			if hash1 == hash2 {
				t.Fatal("hash should differ after adding a column")
			}
			if !strings.Contains(ddl2, "name text") {
				t.Fatalf("expected new column in DDL:\n%s", ddl2)
			}
		})

		t.Run("drop_column", func(t *testing.T) {
			table := "ddl_hash_drop_col"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer PRIMARY KEY, old_col text)", table), table)

			ddl1 := collectDDLString(t, inst.pool)
			hash1 := HashDDL(ddl1)

			if !strings.Contains(ddl1, "old_col") {
				t.Fatalf("expected old_col in initial DDL:\n%s", ddl1)
			}

			if _, err := inst.admin.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN old_col", table)); err != nil {
				t.Fatalf("ALTER TABLE: %v", err)
			}

			ddl2 := collectDDLString(t, inst.pool)
			hash2 := HashDDL(ddl2)

			if hash1 == hash2 {
				t.Fatal("hash should differ after dropping a column")
			}
			if strings.Contains(ddl2, "old_col") {
				t.Fatalf("dropped column should not appear in DDL:\n%s", ddl2)
			}
		})

		t.Run("add_index", func(t *testing.T) {
			table := "ddl_hash_add_idx"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer PRIMARY KEY, val text)", table), table)

			ddl1 := collectDDLString(t, inst.pool)
			hash1 := HashDDL(ddl1)

			if _, err := inst.admin.Exec(ctx, fmt.Sprintf("CREATE INDEX idx_%s_val ON %s (val)", table, table)); err != nil {
				t.Fatalf("CREATE INDEX: %v", err)
			}
			t.Cleanup(func() {
				_, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP INDEX IF EXISTS idx_%s_val", table))
			})

			ddl2 := collectDDLString(t, inst.pool)
			hash2 := HashDDL(ddl2)

			if hash1 == hash2 {
				t.Fatal("hash should differ after adding an index")
			}
			if !strings.Contains(ddl2, fmt.Sprintf("idx_%s_val", table)) {
				t.Fatalf("expected new index in DDL:\n%s", ddl2)
			}
		})

		t.Run("add_constraint", func(t *testing.T) {
			table := "ddl_hash_add_con"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer, val integer)", table), table)

			ddl1 := collectDDLString(t, inst.pool)
			hash1 := HashDDL(ddl1)

			if _, err := inst.admin.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s_val_check CHECK (val > 0)", table, table)); err != nil {
				t.Fatalf("ALTER TABLE ADD CONSTRAINT: %v", err)
			}

			ddl2 := collectDDLString(t, inst.pool)
			hash2 := HashDDL(ddl2)

			if hash1 == hash2 {
				t.Fatal("hash should differ after adding a constraint")
			}
			if !strings.Contains(ddl2, fmt.Sprintf("%s_val_check", table)) {
				t.Fatalf("expected new constraint in DDL:\n%s", ddl2)
			}
		})

		t.Run("idempotent_when_unchanged", func(t *testing.T) {
			table := "ddl_hash_stable"
			execDDL(t, inst.admin, fmt.Sprintf("CREATE TABLE %s (id integer PRIMARY KEY, val text)", table), table)

			ddl1 := collectDDLString(t, inst.pool)
			hash1 := HashDDL(ddl1)

			ddl2 := collectDDLString(t, inst.pool)
			hash2 := HashDDL(ddl2)

			if hash1 != hash2 {
				t.Fatalf("hash should be stable when schema is unchanged: %s != %s", hash1, hash2)
			}
			if ddl1 != ddl2 {
				t.Fatalf("DDL should be identical when schema is unchanged")
			}
		})
	})
}

// TestDDL_MultiVersion runs CollectDDL against all PG versions to ensure no SQL
// compatibility issues.
func TestDDL_MultiVersion(t *testing.T) {
	for _, inst := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", inst.version), func(t *testing.T) {
			table := fmt.Sprintf("ddl_multiversion_%d", inst.version)
			execDDL(t, inst.admin, fmt.Sprintf(`CREATE TABLE %s (
				id integer PRIMARY KEY,
				name text NOT NULL,
				score integer DEFAULT 0,
				created_at timestamptz DEFAULT now()
			)`, table), table)

			ddl := collectDDLString(t, inst.pool)
			if !strings.Contains(ddl, table) {
				t.Fatalf("table %s not found in DDL for PG %d:\n%s", table, inst.version, ddl)
			}
			if !strings.Contains(ddl, "PRIMARY KEY") {
				t.Fatalf("expected PRIMARY KEY in DDL for PG %d:\n%s", inst.version, ddl)
			}
		})
	}
}
