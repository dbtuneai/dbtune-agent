package queries

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

func ptr(s string) *string    { return &s }
func ptrInt32(n int32) *int32 { return &n }

func TestHashDDL_Deterministic(t *testing.T) {
	ddl := "CREATE TABLE foo (id integer);"
	h1 := HashDDL(ddl)
	h2 := HashDDL(ddl)
	if h1 != h2 {
		t.Fatalf("HashDDL not deterministic: %s != %s", h1, h2)
	}
}

func TestHashDDL_Unique(t *testing.T) {
	h1 := HashDDL("CREATE TABLE foo (id integer);")
	h2 := HashDDL("CREATE TABLE bar (id integer);")
	if h1 == h2 {
		t.Fatalf("HashDDL collision: %s", h1)
	}
}

func TestHashDDL_Format(t *testing.T) {
	h := HashDDL("test")
	if len(h) != 64 {
		t.Fatalf("expected 64-char hex, got %d chars: %s", len(h), h)
	}
	expected := fmt.Sprintf("%x", sha256.Sum256([]byte("test")))
	if h != expected {
		t.Fatalf("hash mismatch: got %s, want %s", h, expected)
	}
}

func TestFormatDDLEnumType(t *testing.T) {
	e := DDLEnumType{
		SchemaName: "public",
		TypeName:   "status",
		Labels:     []string{"active", "inactive", "it's complicated"},
	}
	got := formatDDLEnumType(&e)
	want := "CREATE TYPE public.status AS ENUM ('active', 'inactive', 'it''s complicated');"
	if got != want {
		t.Fatalf("got: %s\nwant: %s", got, want)
	}
}

func TestFormatDDLDomainType(t *testing.T) {
	tests := []struct {
		name string
		d    DDLDomainType
		want string
	}{
		{
			name: "simple",
			d:    DDLDomainType{SchemaName: "public", TypeName: "email", BaseType: "text"},
			want: "CREATE DOMAIN public.email AS text;",
		},
		{
			name: "with_not_null_and_default",
			d:    DDLDomainType{SchemaName: "public", TypeName: "posint", BaseType: "integer", NotNull: true, Default: ptr("0")},
			want: "CREATE DOMAIN public.posint AS integer NOT NULL DEFAULT 0;",
		},
		{
			name: "with_constraint",
			d:    DDLDomainType{SchemaName: "public", TypeName: "posint", BaseType: "integer", ConstraintDefs: []string{"CHECK (VALUE > 0)"}},
			want: "CREATE DOMAIN public.posint AS integer CHECK (VALUE > 0);",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDDLDomainType(&tt.d)
			if got != tt.want {
				t.Fatalf("got:  %s\nwant: %s", got, tt.want)
			}
		})
	}
}

func TestFormatCreateSequence(t *testing.T) {
	s := SeqParams{Name: "my_seq", DataType: "bigint", Start: 1, Increment: 1,
		MinValue: 1, MaxValue: 9223372036854775807, Cache: 1}
	got := formatCreateSequence(&s)
	if !strings.Contains(got, "CREATE SEQUENCE my_seq") {
		t.Fatalf("missing CREATE SEQUENCE: %s", got)
	}
	if !strings.Contains(got, "MINVALUE 1") {
		t.Fatalf("missing MINVALUE: %s", got)
	}
	if !strings.Contains(got, "CACHE 1") {
		t.Fatalf("missing CACHE: %s", got)
	}
	if !strings.Contains(got, "NO CYCLE") {
		t.Fatalf("missing NO CYCLE: %s", got)
	}
}

func TestFormatDDLFunction(t *testing.T) {
	f := DDLFunction{
		FunctionDef: "CREATE OR REPLACE FUNCTION public.my_func()\n RETURNS void\n LANGUAGE sql\nAS $function$SELECT 1$function$",
	}
	got := formatDDLFunction(&f)
	if !strings.HasSuffix(got, ";") {
		t.Fatalf("expected trailing semicolon: %s", got)
	}
}

func TestFormatDDLTable_Bare(t *testing.T) {
	// Bare table: no defaults, no constraints, just columns + NOT NULL.
	table := DDLTable{OID: 1, Name: "users", QuotedName: "users", RelKind: "r", Persistence: "p"}
	columns := []DDLColumn{
		{TableOID: 1, AttNum: 1, Name: "id", TypeName: "integer", NotNull: true},
		{TableOID: 1, AttNum: 2, Name: "name", TypeName: "text", NotNull: true},
		{TableOID: 1, AttNum: 3, Name: "bio", TypeName: "text"},
	}
	got := formatDDLTable(&table, columns, nil, nil, nil)
	if !strings.Contains(got, "    id integer NOT NULL") {
		t.Fatalf("missing id NOT NULL: %s", got)
	}
	if !strings.Contains(got, "    name text NOT NULL") {
		t.Fatalf("missing name NOT NULL: %s", got)
	}
	// bio should NOT have NOT NULL
	for _, line := range strings.Split(got, "\n") {
		if strings.Contains(line, "bio") && strings.Contains(line, "NOT NULL") {
			t.Fatalf("bio should not be NOT NULL: %s", line)
		}
	}
	// No DEFAULT, no CONSTRAINT in output
	if strings.Contains(got, "DEFAULT") || strings.Contains(got, "CONSTRAINT") || strings.Contains(got, "PRIMARY") {
		t.Fatalf("bare table should have no defaults or constraints: %s", got)
	}
}

func TestFormatDDLTable_Unlogged(t *testing.T) {
	table := DDLTable{OID: 1, Name: "cache", QuotedName: "cache", RelKind: "r", Persistence: "u"}
	columns := []DDLColumn{{TableOID: 1, AttNum: 1, Name: "id", TypeName: "integer"}}
	got := formatDDLTable(&table, columns, nil, nil, nil)
	if !strings.HasPrefix(got, "CREATE UNLOGGED TABLE") {
		t.Fatalf("expected UNLOGGED: %s", got)
	}
}

func TestFormatDDLTable_Generated(t *testing.T) {
	table := DDLTable{OID: 1, Name: "calc", QuotedName: "calc", RelKind: "r", Persistence: "p"}
	columns := []DDLColumn{
		{TableOID: 1, AttNum: 1, Name: "a", TypeName: "integer"},
		{TableOID: 1, AttNum: 2, Name: "total", TypeName: "integer", Generated: ptr("s"), DefaultExpr: ptr("(a + 1)"), NotNull: true},
	}
	got := formatDDLTable(&table, columns, nil, nil, nil)
	if !strings.Contains(got, "GENERATED ALWAYS AS ((a + 1)) STORED") {
		t.Fatalf("missing generated expression: %s", got)
	}
	if !strings.Contains(got, "NOT NULL") {
		t.Fatalf("missing NOT NULL on generated column: %s", got)
	}
}

func TestFormatSerialDefault(t *testing.T) {
	s := DDLSerialSequence{TableName: "users", ColumnName: "id", DefaultExpr: "nextval('users_id_seq'::regclass)"}
	got := formatSerialDefault(&s)
	want := "ALTER TABLE ONLY users ALTER COLUMN id SET DEFAULT nextval('users_id_seq'::regclass);"
	if got != want {
		t.Fatalf("got:  %s\nwant: %s", got, want)
	}
}

func TestFormatSequenceOwnedBy(t *testing.T) {
	s := DDLSerialSequence{SeqParams: SeqParams{Name: "users_id_seq"}, TableName: "users", ColumnName: "id"}
	got := formatSequenceOwnedBy(&s)
	want := "ALTER SEQUENCE users_id_seq OWNED BY users.id;"
	if got != want {
		t.Fatalf("got:  %s\nwant: %s", got, want)
	}
}

func TestFormatIdentityColumn(t *testing.T) {
	s := DDLIdentitySequence{
		SeqParams:    SeqParams{Name: "users_id_seq", Start: 1, Increment: 1, MinValue: 1, MaxValue: 2147483647, Cache: 1},
		TableName:    "users",
		ColumnName:   "id",
		IdentityKind: "a",
	}
	got := formatIdentityColumn(&s)
	if !strings.Contains(got, "ADD GENERATED ALWAYS AS IDENTITY") {
		t.Fatalf("missing GENERATED ALWAYS: %s", got)
	}
	if !strings.Contains(got, "SEQUENCE NAME users_id_seq") {
		t.Fatalf("missing SEQUENCE NAME: %s", got)
	}
}

func TestFormatColumnSetStorage(t *testing.T) {
	col := DDLColumn{TableName: "users", Name: "bio", Storage: ptr("e")}
	got := formatColumnSetStorage(&col)
	want := "ALTER TABLE ONLY users ALTER COLUMN bio SET STORAGE EXTERNAL;"
	if got != want {
		t.Fatalf("got:  %s\nwant: %s", got, want)
	}
}

func TestFormatColumnSetStatistics(t *testing.T) {
	col := DDLColumn{TableName: "users", Name: "score", Statistics: ptrInt32(1000)}
	got := formatColumnSetStatistics(&col)
	want := "ALTER TABLE ONLY users ALTER COLUMN score SET STATISTICS 1000;"
	if got != want {
		t.Fatalf("got:  %s\nwant: %s", got, want)
	}
}

func TestFormatDDLConstraint(t *testing.T) {
	c := DDLConstraint{
		QuotedTable: "users", QuotedConName: "users_pkey",
		ConType: "p", ConstraintDef: "PRIMARY KEY (id)",
	}
	got := formatDDLConstraint(&c)
	want := "ALTER TABLE ONLY users\n    ADD CONSTRAINT users_pkey PRIMARY KEY (id);"
	if got != want {
		t.Fatalf("got:  %s\nwant: %s", got, want)
	}
}

func TestFormatDDLForeignKey(t *testing.T) {
	fk := DDLForeignKey{
		TableName: "orders", ConName: "orders_user_id_fkey",
		ConstraintDef: "FOREIGN KEY (user_id) REFERENCES users(id)",
	}
	got := formatDDLForeignKey(&fk)
	if !strings.Contains(got, "ALTER TABLE ONLY orders") {
		t.Fatalf("missing ALTER TABLE ONLY: %s", got)
	}
	if !strings.Contains(got, "ADD CONSTRAINT orders_user_id_fkey") {
		t.Fatalf("missing ADD CONSTRAINT: %s", got)
	}
}

func TestFormatDDLView(t *testing.T) {
	v := DDLView{Name: "active_users", ViewDef: " SELECT id FROM users"}
	got := formatDDLView(&v)
	if !strings.HasPrefix(got, "CREATE VIEW active_users AS") {
		t.Fatalf("unexpected prefix: %s", got)
	}
	if !strings.HasSuffix(got, ";") {
		t.Fatalf("expected trailing semicolon: %s", got)
	}
}

func TestFormatDDLView_WithOptions(t *testing.T) {
	v := DDLView{Name: "secure_v", ViewDef: " SELECT 1", RelOptions: ptr("security_barrier=true")}
	got := formatDDLView(&v)
	if !strings.Contains(got, "WITH (security_barrier=true)") {
		t.Fatalf("missing view options: %s", got)
	}
}

func TestFormatDDLMaterializedView(t *testing.T) {
	mv := DDLMaterializedView{Name: "stats", ViewDef: " SELECT count(*) FROM users"}
	got := formatDDLMaterializedView(&mv)
	if !strings.Contains(got, "CREATE MATERIALIZED VIEW stats") {
		t.Fatalf("unexpected: %s", got)
	}
	if !strings.HasSuffix(got, "WITH NO DATA;") {
		t.Fatalf("expected WITH NO DATA suffix: %s", got)
	}
}

func TestFormatDDLRLSPolicy_WithRoles(t *testing.T) {
	p := DDLRLSPolicy{
		PolicyName: "read_own", TableName: "secrets", Command: "r",
		Permissive: true, Roles: []string{"admin", "viewer"},
		UsingExpr: ptr("(owner = current_user)"),
	}
	got := formatDDLRLSPolicy(&p)
	if !strings.Contains(got, "TO admin, viewer") {
		t.Fatalf("missing TO roles: %s", got)
	}
}

func TestFormatDDLRLSPolicy_PublicOmitted(t *testing.T) {
	p := DDLRLSPolicy{
		PolicyName: "read_all", TableName: "docs", Command: "r",
		Permissive: true, Roles: []string{"PUBLIC"},
		UsingExpr: ptr("true"),
	}
	got := formatDDLRLSPolicy(&p)
	if strings.Contains(got, " TO ") {
		t.Fatalf("TO PUBLIC should be omitted: %s", got)
	}
}

func TestFormatDDLTrigger(t *testing.T) {
	tr := DDLTrigger{TriggerDef: "CREATE TRIGGER trg BEFORE UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION update_ts()"}
	got := formatDDLTrigger(&tr)
	want := "CREATE TRIGGER trg BEFORE UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION update_ts();"
	if got != want {
		t.Fatalf("got:  %s\nwant: %s", got, want)
	}
}

func TestQuoteStringLiteral(t *testing.T) {
	tests := []struct{ in, want string }{
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{"", "''"},
	}
	for _, tt := range tests {
		got := quoteStringLiteral(tt.in)
		if got != tt.want {
			t.Fatalf("quoteStringLiteral(%q) = %s, want %s", tt.in, got, tt.want)
		}
	}
}
