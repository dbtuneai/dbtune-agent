//go:build integration

package pgxutil

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type pgInstance struct {
	connStr string
	version int
}

var pgInstances []pgInstance

func TestMain(m *testing.M) {
	var logBuf bytes.Buffer
	tclog.SetDefault(&bufLogger{buf: &logBuf})
	log.SetOutput(&logBuf)

	ctx := context.Background()
	versions := []int{13, 14, 15, 16, 17, 18}

	containers := make([]*postgres.PostgresContainer, 0, len(versions))

	cleanup := func() {
		for _, c := range containers {
			_ = c.Terminate(ctx)
		}
	}

	for _, v := range versions {
		image := fmt.Sprintf("postgres:%d-alpine", v)
		hostPort := fmt.Sprintf("%d", 45300+v)

		pgContainer, err := postgres.Run(ctx, image,
			postgres.WithDatabase("testdb"),
			postgres.WithUsername("test"),
			postgres.WithPassword("test"),
			testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					HostConfigModifier: func(hc *container.HostConfig) {
						hc.PortBindings = nat.PortMap{
							"5432/tcp": []nat.PortBinding{
								{HostIP: "127.0.0.1", HostPort: hostPort},
							},
						}
					},
				},
			}),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(60*time.Second),
			),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to start postgres %d on port %s: %v\n", v, hostPort, err)
			fmt.Fprintf(os.Stderr, "--- testcontainers log ---\n%s", logBuf.String())
			cleanup()
			os.Exit(1)
		}
		containers = append(containers, pgContainer)

		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get connection string for postgres %d: %v\n", v, err)
			cleanup()
			os.Exit(1)
		}

		pgInstances = append(pgInstances, pgInstance{connStr: connStr, version: v})
	}

	code := m.Run()
	cleanup()

	if code != 0 {
		fmt.Fprintf(os.Stderr, "--- testcontainers log ---\n%s", logBuf.String())
	}

	os.Exit(code)
}

type bufLogger struct {
	buf *bytes.Buffer
}

func (l *bufLogger) Printf(format string, v ...any) {
	fmt.Fprintf(l.buf, format+"\n", v...)
}

// latestInstance returns the highest PG version instance.
func latestInstance() pgInstance {
	return pgInstances[len(pgInstances)-1]
}

func connectForTest(t *testing.T, ctx context.Context) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, latestInstance().connStr)
	if err != nil {
		t.Fatalf("cannot connect to test database: %v", err)
	}
	return conn
}

func connectToVersion(t *testing.T, ctx context.Context, inst pgInstance) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, inst.connStr)
	if err != nil {
		t.Fatalf("cannot connect to PG %d: %v", inst.version, err)
	}
	return conn
}

// --- Core laxness ---

func TestScan_ExtraColumnsSkipped(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type small struct {
		Name string `db:"name"`
		Age  int32  `db:"age"`
	}
	scanner := NewScanner[small]()

	rows, _ := conn.Query(ctx,
		`SELECT 'Alice' AS name, 30 AS age, 'ignored' AS extra_col`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].Name != "Alice" || result[0].Age != 30 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestScan_MissingColumnsZeroValued(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type wide struct {
		Name    string  `db:"name"`
		Age     int32   `db:"age"`
		Missing float64 `db:"does_not_exist"`
	}
	scanner := NewScanner[wide]()

	rows, _ := conn.Query(ctx, `SELECT 'Bob' AS name, 25 AS age`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Missing != 0 {
		t.Fatalf("expected Missing=0, got %f", result[0].Missing)
	}
}

func TestScan_BothDirections(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type mixed struct {
		Name     string `db:"name"`
		NotInRow int64  `db:"not_in_row"`
	}
	scanner := NewScanner[mixed]()

	rows, _ := conn.Query(ctx, `SELECT 'Carol' AS name, 99 AS extra`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Name != "Carol" || result[0].NotInRow != 0 {
		t.Fatalf("unexpected: %+v", result[0])
	}
}

// --- Struct field control ---

func TestScan_DbTagIgnore(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type withIgnored struct {
		Name   string `db:"name"`
		Age    int32  `db:"age"`
		Ignore bool   `db:"-"`
	}
	scanner := NewScanner[withIgnored]()

	t.Run("column absent", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'Dave' AS name, 30 AS age`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].Name != "Dave" || result[0].Age != 30 {
			t.Fatalf("unexpected values: %+v", result[0])
		}
		if result[0].Ignore != false {
			t.Fatal("ignored field should be zero")
		}
	})

	t.Run("column present but ignored", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'Eve' AS name, 25 AS age, true AS ignore`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].Name != "Eve" || result[0].Age != 25 {
			t.Fatalf("unexpected values: %+v", result[0])
		}
		if result[0].Ignore != false {
			t.Fatal("ignored field should be zero even though column was returned")
		}
	})

	t.Run("column absent again after being present", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'Frank' AS name, 40 AS age`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].Name != "Frank" || result[0].Age != 40 || result[0].Ignore != false {
			t.Fatalf("unexpected: %+v", result[0])
		}
	})
}

func TestScan_DbTagRename(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type renamed struct {
		TransactionsCommitted  int64 `db:"xact_commit"`
		TransactionsRolledBack int64 `db:"xact_rollback"`
	}
	scanner := NewScanner[renamed]()

	rows, _ := conn.Query(ctx, `SELECT 100 AS xact_commit, 5 AS xact_rollback`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].TransactionsCommitted != 100 || result[0].TransactionsRolledBack != 5 {
		t.Fatalf("tag rename failed: %+v", result[0])
	}
}

func TestScan_CaseInsensitiveMatch(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type cased struct {
		Name string `db:"name"`
		Age  int32  `db:"age"`
	}
	scanner := NewScanner[cased]()

	rows, _ := conn.Query(ctx, `SELECT 'Alice' AS "Name", 30 AS "AGE"`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Name != "Alice" || result[0].Age != 30 {
		t.Fatalf("case-insensitive match failed: %+v", result[0])
	}
}

func TestScan_UnexportedFieldsIgnored(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type withUnexported struct {
		Name   string `db:"name"`
		secret string //nolint:unused,nolintlint
	}
	scanner := NewScanner[withUnexported]()

	rows, _ := conn.Query(ctx, `SELECT 'visible' AS name, 'hidden' AS secret`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Name != "visible" {
		t.Fatalf("unexpected: %+v", result[0])
	}
	if result[0].secret != "" {
		t.Fatal("unexported field should not have been written to")
	}
}

func TestScan_NoMatchingFieldsAtAll(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type disjoint struct {
		Foo string `db:"foo"`
		Bar int32  `db:"bar"`
	}
	scanner := NewScanner[disjoint]()

	rows, _ := conn.Query(ctx, `SELECT 1 AS x, 2 AS y, 3 AS z`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Foo != "" || result[0].Bar != 0 {
		t.Fatalf("all fields should be zero: %+v", result[0])
	}
}

// --- Embedded structs ---

func TestScan_EmbeddedStruct(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type Base struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	type Extended struct {
		Base
		Extra string `db:"extra"`
	}
	scanner := NewScanner[Extended]()

	rows, _ := conn.Query(ctx, `SELECT 1 AS id, 'test' AS name, 'bonus' AS extra`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].ID != 1 || result[0].Name != "test" || result[0].Extra != "bonus" {
		t.Fatalf("unexpected: %+v", result[0])
	}
}

func TestScan_DeeplyNestedEmbed(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type BaseFields struct {
		Datname  string `db:"datname"`
		Backends int32  `db:"numbackends"`
	}
	type PG14Fields struct {
		BaseFields
		SessionTime float64 `db:"session_time"`
	}
	type PG17Fields struct {
		PG14Fields
		ParallelWorkers int64 `db:"parallel_workers"`
	}
	scanner := NewScanner[PG17Fields]()

	t.Run("all columns present", func(t *testing.T) {
		rows, _ := conn.Query(ctx,
			`SELECT 'mydb' AS datname, 5 AS numbackends, 100.5 AS session_time, 8 AS parallel_workers`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		r := result[0]
		if r.Datname != "mydb" || r.Backends != 5 || r.SessionTime != 100.5 || r.ParallelWorkers != 8 {
			t.Fatalf("unexpected: %+v", r)
		}
	})

	t.Run("only base columns present", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'olddb' AS datname, 3 AS numbackends`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		r := result[0]
		if r.Datname != "olddb" || r.Backends != 3 {
			t.Fatalf("base fields wrong: %+v", r)
		}
		if r.SessionTime != 0 || r.ParallelWorkers != 0 {
			t.Fatalf("nested fields should be zero: %+v", r)
		}
	})
}

func TestScan_EmbeddedWithIgnoredField(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type Inner struct {
		Keep string `db:"keep"`
		Skip string `db:"-"`
	}
	type Outer struct {
		Inner
		Extra int32 `db:"extra"`
	}
	scanner := NewScanner[Outer]()

	rows, _ := conn.Query(ctx, `SELECT 'kept' AS keep, 'skipped' AS skip, 42 AS extra`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Keep != "kept" || result[0].Extra != 42 {
		t.Fatalf("unexpected: %+v", result[0])
	}
	if result[0].Skip != "" {
		t.Fatal("inner db:\"-\" field should be zero")
	}
}

func TestScan_EmbeddedWithMissingColumns(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type CoreFields struct {
		Datname string `db:"datname"`
	}
	type PG14Fields struct {
		SessionTime float64 `db:"session_time"`
		ActiveTime  float64 `db:"active_time"`
	}
	type Combined struct {
		CoreFields
		PG14Fields
	}
	scanner := NewScanner[Combined]()

	rows, _ := conn.Query(ctx, `SELECT 'pg12db' AS datname`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r := result[0]
	if r.Datname != "pg12db" {
		t.Fatalf("core field wrong: %+v", r)
	}
	if r.SessionTime != 0 || r.ActiveTime != 0 {
		t.Fatalf("PG14 fields should be zero on PG12: %+v", r)
	}
}

// --- PG type handling ---

func TestScan_NullIntoPointer(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type withPointers struct {
		Name      string  `db:"name"`
		NullableN *int64  `db:"nullable_n"`
		NullableS *string `db:"nullable_s"`
	}
	scanner := NewScanner[withPointers]()

	t.Run("null values", func(t *testing.T) {
		rows, _ := conn.Query(ctx,
			`SELECT 'test' AS name, NULL::bigint AS nullable_n, NULL::text AS nullable_s`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].NullableN != nil || result[0].NullableS != nil {
			t.Fatalf("expected nils: %+v", result[0])
		}
	})

	t.Run("non-null values", func(t *testing.T) {
		rows, _ := conn.Query(ctx,
			`SELECT 'test' AS name, 42::bigint AS nullable_n, 'hello'::text AS nullable_s`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].NullableN == nil || *result[0].NullableN != 42 {
			t.Fatalf("expected 42: %+v", result[0])
		}
		if result[0].NullableS == nil || *result[0].NullableS != "hello" {
			t.Fatalf("expected 'hello': %+v", result[0])
		}
	})
}

func TestScan_NullIntoNonPointer(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type badNullable struct {
		Val int64 `db:"val"`
	}
	scanner := NewScanner[badNullable]()

	rows, _ := conn.Query(ctx, `SELECT NULL::bigint AS val`)
	_, err := pgx.CollectRows(rows, scanner.Scan)
	if err == nil {
		t.Fatal("expected error scanning NULL into non-pointer int64")
	}
}

func TestScan_TypeCoercions(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type pgTypes struct {
		SmallInt int32   `db:"i32"`
		BigInt   int64   `db:"i64"`
		Double   float64 `db:"f64"`
		Text     string  `db:"txt"`
		Flag     bool    `db:"flag"`
	}
	scanner := NewScanner[pgTypes]()

	rows, _ := conn.Query(ctx,
		`SELECT 1::int AS i32, 9999999999::bigint AS i64, 3.14::float8 AS f64, 'hello'::text AS txt, true AS flag`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r := result[0]
	if r.SmallInt != 1 || r.BigInt != 9999999999 || r.Double != 3.14 || r.Text != "hello" || r.Flag != true {
		t.Fatalf("type coercion failed: %+v", r)
	}
}

func TestScan_BoolColumn(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type withBool struct {
		Name   string `db:"name"`
		Active bool   `db:"active"`
	}
	scanner := NewScanner[withBool]()

	t.Run("bool present true", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'a' AS name, true AS active`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result[0].Active {
			t.Fatal("expected true")
		}
	})

	t.Run("bool present false", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'b' AS name, false AS active`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].Active {
			t.Fatal("expected false")
		}
	})

	t.Run("bool column absent", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'c' AS name`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].Active {
			t.Fatal("missing bool should default to false")
		}
	})
}

// --- Version transitions ---

func TestScan_Upgrade(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type versioned struct {
		A string `db:"a"`
		B int32  `db:"b"`
		C int64  `db:"c"`
		D string `db:"d"`
	}
	scanner := NewScanner[versioned]()

	rows1, _ := conn.Query(ctx, `SELECT 'v1' AS a, 1 AS b`)
	r1, err := pgx.CollectRows(rows1, scanner.Scan)
	if err != nil {
		t.Fatalf("phase 1: %v", err)
	}
	if r1[0].A != "v1" || r1[0].B != 1 || r1[0].C != 0 || r1[0].D != "" {
		t.Fatalf("phase 1 unexpected: %+v", r1[0])
	}

	rows2, _ := conn.Query(ctx, `SELECT 'v2' AS a, 2 AS b, 42 AS c`)
	r2, err := pgx.CollectRows(rows2, scanner.Scan)
	if err != nil {
		t.Fatalf("phase 2: %v", err)
	}
	if r2[0].A != "v2" || r2[0].B != 2 || r2[0].C != 42 || r2[0].D != "" {
		t.Fatalf("phase 2 unexpected: %+v", r2[0])
	}

	rows3, _ := conn.Query(ctx, `SELECT 'v3' AS a, 3 AS b, 99 AS c, 'new' AS d, true AS e`)
	r3, err := pgx.CollectRows(rows3, scanner.Scan)
	if err != nil {
		t.Fatalf("phase 3: %v", err)
	}
	if r3[0].A != "v3" || r3[0].B != 3 || r3[0].C != 99 || r3[0].D != "new" {
		t.Fatalf("phase 3 unexpected: %+v", r3[0])
	}
}

func TestScan_Downgrade(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type versioned struct {
		A string `db:"a"`
		B int32  `db:"b"`
		C int64  `db:"c"`
		D string `db:"d"`
	}
	scanner := NewScanner[versioned]()

	rows1, _ := conn.Query(ctx, `SELECT 'wide' AS a, 10 AS b, 77 AS c, 'full' AS d`)
	r1, err := pgx.CollectRows(rows1, scanner.Scan)
	if err != nil {
		t.Fatalf("phase 1: %v", err)
	}
	if r1[0].A != "wide" || r1[0].B != 10 || r1[0].C != 77 || r1[0].D != "full" {
		t.Fatalf("phase 1 unexpected: %+v", r1[0])
	}

	rows2, _ := conn.Query(ctx, `SELECT 'narrow' AS a, 5 AS b`)
	r2, err := pgx.CollectRows(rows2, scanner.Scan)
	if err != nil {
		t.Fatalf("phase 2: %v", err)
	}
	if r2[0].A != "narrow" || r2[0].B != 5 {
		t.Fatalf("phase 2 values wrong: %+v", r2[0])
	}
	if r2[0].C != 0 || r2[0].D != "" {
		t.Fatalf("phase 2: missing fields should be zero, got C=%d D=%q", r2[0].C, r2[0].D)
	}
}

func TestScan_UpgradeDowngradeCycle(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type versioned struct {
		A string `db:"a"`
		B int32  `db:"b"`
		C int64  `db:"c"`
	}
	scanner := NewScanner[versioned]()

	run := func(phase string, query string, wantA string, wantB int32, wantC int64) {
		t.Helper()
		rows, _ := conn.Query(ctx, query)
		results, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		if len(results) != 1 {
			t.Fatalf("%s: expected 1 row, got %d", phase, len(results))
		}
		r := results[0]
		if r.A != wantA || r.B != wantB || r.C != wantC {
			t.Fatalf("%s: got A=%q B=%d C=%d, want A=%q B=%d C=%d",
				phase, r.A, r.B, r.C, wantA, wantB, wantC)
		}
	}

	run("old-1", `SELECT 'a1' AS a, 1 AS b`, "a1", 1, 0)
	run("new-1", `SELECT 'a2' AS a, 2 AS b, 20 AS c`, "a2", 2, 20)
	run("old-2", `SELECT 'a3' AS a, 3 AS b`, "a3", 3, 0)
	run("new-2", `SELECT 'a4' AS a, 4 AS b, 40 AS c`, "a4", 4, 40)
}

func TestScan_ColumnOrderChange(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type ordered struct {
		X string `db:"x"`
		Y int32  `db:"y"`
		Z int64  `db:"z"`
	}
	scanner := NewScanner[ordered]()

	t.Run("order x,y,z", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'a' AS x, 1 AS y, 10 AS z`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].X != "a" || result[0].Y != 1 || result[0].Z != 10 {
			t.Fatalf("unexpected: %+v", result[0])
		}
	})

	t.Run("order z,x,y", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 20 AS z, 'b' AS x, 2 AS y`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].X != "b" || result[0].Y != 2 || result[0].Z != 20 {
			t.Fatalf("unexpected: %+v", result[0])
		}
	})

	t.Run("order y,z,x", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 3 AS y, 30 AS z, 'c' AS x`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].X != "c" || result[0].Y != 3 || result[0].Z != 30 {
			t.Fatalf("unexpected: %+v", result[0])
		}
	})
}

// --- Multi-row and empty results ---

func TestScan_EmptyResultSet(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type row struct {
		N int32 `db:"n"`
	}
	scanner := NewScanner[row]()

	rows, _ := conn.Query(ctx, `SELECT 1 AS n WHERE false`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty slice, got %d rows", len(result))
	}
}

func TestScan_MultipleRows(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type row struct {
		N     int32  `db:"n"`
		Label string `db:"label"`
	}
	scanner := NewScanner[row]()

	rows, _ := conn.Query(ctx,
		`SELECT n, 'row_' || n::text AS label FROM generate_series(1, 5) AS n`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result))
	}
	for i, r := range result {
		expectedN := int32(i + 1)
		expectedLabel := fmt.Sprintf("row_%d", expectedN)
		if r.N != expectedN || r.Label != expectedLabel {
			t.Fatalf("row %d: got N=%d Label=%q, want N=%d Label=%q",
				i, r.N, r.Label, expectedN, expectedLabel)
		}
	}
}

func TestScan_MultipleRowsWithNulls(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type row struct {
		ID   int32   `db:"id"`
		Name *string `db:"name"`
	}
	scanner := NewScanner[row]()

	rows, _ := conn.Query(ctx, `
		SELECT 1 AS id, 'alice'::text AS name
		UNION ALL
		SELECT 2 AS id, NULL::text AS name
		UNION ALL
		SELECT 3 AS id, 'charlie'::text AS name
	`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}

	if result[0].ID != 1 || result[0].Name == nil || *result[0].Name != "alice" {
		t.Fatalf("row 0 unexpected: %+v", result[0])
	}
	if result[1].ID != 2 || result[1].Name != nil {
		t.Fatalf("row 1 should have nil Name: %+v", result[1])
	}
	if result[2].ID != 3 || result[2].Name == nil || *result[2].Name != "charlie" {
		t.Fatalf("row 2 unexpected: %+v", result[2])
	}
}

// --- Concurrency ---

func TestScan_ConcurrentSameLayout(t *testing.T) {
	ctx := context.Background()
	connStr := latestInstance().connStr

	type row struct {
		N int64 `db:"n"`
	}
	scanner := NewScanner[row]()

	// Pre-warm the cache so the test focuses on concurrent read path.
	warm := connectForTest(t, ctx)
	warmRows, _ := warm.Query(ctx, `SELECT 0::bigint AS n`)
	_, _ = pgx.CollectRows(warmRows, scanner.Scan)
	warm.Close(ctx)

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := pgx.Connect(ctx, connStr)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d connect: %w", i, err)
				return
			}
			defer conn.Close(ctx)

			rows, err := conn.Query(ctx, `SELECT $1::bigint AS n`, int64(i))
			if err != nil {
				errs <- fmt.Errorf("goroutine %d query: %w", i, err)
				return
			}
			result, err := pgx.CollectRows(rows, scanner.Scan)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d collect: %w", i, err)
				return
			}
			if len(result) != 1 || result[0].N != int64(i) {
				errs <- fmt.Errorf("goroutine %d: got %+v, want N=%d", i, result, i)
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestScan_ConcurrentDifferentLayouts(t *testing.T) {
	ctx := context.Background()
	connStr := latestInstance().connStr

	type row struct {
		A string `db:"a"`
		B int32  `db:"b"`
		C int64  `db:"c"`
	}
	scanner := NewScanner[row]()

	queries := []struct {
		sql   string
		wantA string
		wantB int32
		wantC int64
	}{
		{`SELECT 'v1' AS a, 1 AS b`, "v1", 1, 0},
		{`SELECT 'v2' AS a, 2 AS b, 20 AS c`, "v2", 2, 20},
		{`SELECT 'v3' AS a, 3 AS b, 30 AS c, true AS extra`, "v3", 3, 30},
	}

	const iterations = 10
	var wg sync.WaitGroup
	errs := make(chan error, len(queries)*iterations)

	for _, q := range queries {
		for j := 0; j < iterations; j++ {
			wg.Add(1)
			go func(sql, wantA string, wantB int32, wantC int64) {
				defer wg.Done()
				conn, err := pgx.Connect(ctx, connStr)
				if err != nil {
					errs <- fmt.Errorf("connect for %q: %w", sql, err)
					return
				}
				defer conn.Close(ctx)

				rows, err := conn.Query(ctx, sql)
				if err != nil {
					errs <- fmt.Errorf("query %q: %w", sql, err)
					return
				}
				result, err := pgx.CollectRows(rows, scanner.Scan)
				if err != nil {
					errs <- fmt.Errorf("collect %q: %w", sql, err)
					return
				}
				if len(result) != 1 {
					errs <- fmt.Errorf("%q: expected 1 row", sql)
					return
				}
				r := result[0]
				if r.A != wantA || r.B != wantB || r.C != wantC {
					errs <- fmt.Errorf("%q: got %+v, want A=%q B=%d C=%d",
						sql, r, wantA, wantB, wantC)
				}
			}(q.sql, q.wantA, q.wantB, q.wantC)
		}
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// --- Multi-version: real system views ---

type PgStatDatabase struct {
	Datid        uint32     `db:"datid"`
	Datname      *string    `db:"datname"`
	Numbackends  int32      `db:"numbackends"`
	XactCommit   int64      `db:"xact_commit"`
	XactRollback int64      `db:"xact_rollback"`
	BlksRead     int64      `db:"blks_read"`
	BlksHit      int64      `db:"blks_hit"`
	TupReturned  int64      `db:"tup_returned"`
	TupFetched   int64      `db:"tup_fetched"`
	TupInserted  int64      `db:"tup_inserted"`
	TupUpdated   int64      `db:"tup_updated"`
	TupDeleted   int64      `db:"tup_deleted"`
	Conflicts    int64      `db:"conflicts"`
	TempFiles    int64      `db:"temp_files"`
	TempBytes    int64      `db:"temp_bytes"`
	Deadlocks    int64      `db:"deadlocks"`
	BlkReadTime  float64    `db:"blk_read_time"`
	BlkWriteTime float64    `db:"blk_write_time"`
	StatsReset   *time.Time `db:"stats_reset"`

	// PG 12+
	ChecksumFailures    *int64     `db:"checksum_failures"`
	ChecksumLastFailure *time.Time `db:"checksum_last_failure"`

	// PG 14+
	SessionTime           float64 `db:"session_time"`
	ActiveTime            float64 `db:"active_time"`
	IdleInTransactionTime float64 `db:"idle_in_transaction_time"`
	Sessions              int64   `db:"sessions"`
	SessionsAbandoned     int64   `db:"sessions_abandoned"`
	SessionsFatal         int64   `db:"sessions_fatal"`
	SessionsKilled        int64   `db:"sessions_killed"`

	// PG 17+
	ParallelWorkers  int64 `db:"parallel_workers"`
	TempBytesRead    int64 `db:"temp_bytes_read"`
	TempBytesWritten int64 `db:"temp_bytes_written"`
	TempFilesRead    int64 `db:"temp_files_read"`
	TempFilesWritten int64 `db:"temp_files_written"`
}

var pgStatScanner = NewScanner[PgStatDatabase]()

// TestScan_PgStatDatabase runs SELECT * FROM pg_stat_database against all PG versions,
// verifying that the scanner handles version-specific columns gracefully.
func TestScan_PgStatDatabase(t *testing.T) {
	ctx := context.Background()

	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", pg.version), func(t *testing.T) {
			conn := connectToVersion(t, ctx, pg)
			defer conn.Close(ctx)

			rows, err := conn.Query(ctx, "SELECT * FROM pg_stat_database")
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			results, err := pgx.CollectRows(rows, pgStatScanner.Scan)
			if err != nil {
				t.Fatalf("collect failed: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("expected at least one database")
			}
			found := false
			for _, r := range results {
				if r.Datname != nil && *r.Datname != "" {
					found = true
					break
				}
			}
			if !found {
				t.Fatal("expected at least one row with a non-empty datname")
			}
		})
	}
}

func TestScan_PgStatActivity(t *testing.T) {
	ctx := context.Background()

	type PgStatActivity struct {
		Datname     *string `db:"datname"`
		Pid         int32   `db:"pid"`
		State       *string `db:"state"`
		BackendType *string `db:"backend_type"`
		QueryID     *int64  `db:"query_id"`
	}
	scanner := NewScanner[PgStatActivity]()

	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", pg.version), func(t *testing.T) {
			conn := connectToVersion(t, ctx, pg)
			defer conn.Close(ctx)

			rows, err := conn.Query(ctx, "SELECT * FROM pg_stat_activity LIMIT 5")
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			results, err := pgx.CollectRows(rows, scanner.Scan)
			if err != nil {
				t.Fatalf("collect failed: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("expected at least one row from pg_stat_activity")
			}
			foundSelf := false
			for _, r := range results {
				if r.Pid > 0 {
					foundSelf = true
					break
				}
			}
			if !foundSelf {
				t.Fatal("expected at least one row with a valid pid")
			}
		})
	}
}

// --- Defined types ---

func TestScan_DefinedTypes(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type Oid uint32
	type Name string
	type Text string
	type Integer int64
	type Bigint int64
	type Real float64
	type DoublePrecision float64
	type Boolean bool
	type definedRow struct {
		ID      Oid             `db:"id"`
		Name    Name            `db:"name"`
		Label   Text            `db:"label"`
		Count   Integer         `db:"count"`
		Total   Bigint          `db:"total"`
		Ratio   Real            `db:"ratio"`
		Precise DoublePrecision `db:"precise"`
		Active  Boolean         `db:"active"`
	}
	scanner := NewScanner[definedRow]()

	rows, _ := conn.Query(ctx, `SELECT
		42::oid AS id,
		'myname'::name AS name,
		'some text'::text AS label,
		7::int4 AS count,
		9999999999::int8 AS total,
		1.5::float4 AS ratio,
		3.14159::float8 AS precise,
		true AS active`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error scanning into defined types: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	r := result[0]
	if r.ID != 42 {
		t.Errorf("ID: got %d, want 42", r.ID)
	}
	if r.Name != "myname" {
		t.Errorf("Name: got %q, want %q", r.Name, "myname")
	}
	if r.Label != "some text" {
		t.Errorf("Label: got %q, want %q", r.Label, "some text")
	}
	if r.Count != 7 {
		t.Errorf("Count: got %d, want 7", r.Count)
	}
	if r.Total != 9999999999 {
		t.Errorf("Total: got %d, want 9999999999", r.Total)
	}
	if r.Active != true {
		t.Errorf("Active: got %v, want true", r.Active)
	}
}

func TestScan_DefinedTypePointers(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type Oid uint32
	type Name string
	type Integer int64
	type TimestampTZ string

	type nullableRow struct {
		ID    *Oid         `db:"id"`
		Name  *Name        `db:"name"`
		Count *Integer     `db:"count"`
		TS    *TimestampTZ `db:"ts"`
	}
	scanner := NewScanner[nullableRow]()

	t.Run("non-null values", func(t *testing.T) {
		rows, _ := conn.Query(ctx,
			`SELECT 1::oid AS id, 'test'::name AS name, 5::int4 AS count, now()::timestamptz::text AS ts`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		r := result[0]
		if r.ID == nil || *r.ID != 1 {
			t.Errorf("ID: got %v, want 1", r.ID)
		}
		if r.Name == nil || *r.Name != "test" {
			t.Errorf("Name: got %v, want 'test'", r.Name)
		}
		if r.Count == nil || *r.Count != 5 {
			t.Errorf("Count: got %v, want 5", r.Count)
		}
		if r.TS == nil || *r.TS == "" {
			t.Errorf("TS: got %v, want non-empty", r.TS)
		}
	})

	t.Run("null values", func(t *testing.T) {
		rows, _ := conn.Query(ctx,
			`SELECT NULL::oid AS id, NULL::name AS name, NULL::int4 AS count, NULL::text AS ts`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		r := result[0]
		if r.ID != nil {
			t.Errorf("ID should be nil, got %v", r.ID)
		}
		if r.Name != nil {
			t.Errorf("Name should be nil, got %v", r.Name)
		}
		if r.Count != nil {
			t.Errorf("Count should be nil, got %v", r.Count)
		}
		if r.TS != nil {
			t.Errorf("TS should be nil, got %v", r.TS)
		}
	})
}
