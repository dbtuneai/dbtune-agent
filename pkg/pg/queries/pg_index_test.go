package queries

import (
	"testing"
)

func mkRow(schema, name string, relPages int32) PgIndexRow {
	s := Name(schema)
	n := Name(name)
	rp := Integer(relPages)
	return PgIndexRow{
		SchemaName: &s,
		IndexName:  &n,
		RelPages:   &rp,
	}
}

func TestPgIndexState_FirstCallEmitsAll(t *testing.T) {
	st := newPgIndexState(1000)
	rows := []PgIndexRow{mkRow("public", "a", 1), mkRow("public", "b", 1)}

	out, err := st.filter(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("first call should emit all rows, got %d", len(out))
	}
}

func TestPgIndexState_UnchangedSecondCallEmitsNone(t *testing.T) {
	st := newPgIndexState(1000)
	rows := []PgIndexRow{mkRow("public", "a", 1), mkRow("public", "b", 1)}

	if _, err := st.filter(rows); err != nil {
		t.Fatal(err)
	}
	out, err := st.filter(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("identical second call should emit nothing, got %d", len(out))
	}
}

func TestPgIndexState_OnlyChangedRowEmitted(t *testing.T) {
	st := newPgIndexState(1000)
	first := []PgIndexRow{mkRow("public", "a", 1), mkRow("public", "b", 1)}
	if _, err := st.filter(first); err != nil {
		t.Fatal(err)
	}

	second := []PgIndexRow{mkRow("public", "a", 1), mkRow("public", "b", 2)}
	out, err := st.filter(second)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || string(*out[0].IndexName) != "b" {
		t.Fatalf("expected only 'b' in output, got %#v", out)
	}
}

func TestPgIndexState_HeartbeatResendsAll(t *testing.T) {
	const heartbeat = 3
	st := newPgIndexState(heartbeat)
	rows := []PgIndexRow{mkRow("public", "a", 1)}

	if out, _ := st.filter(rows); len(out) != 1 {
		t.Fatalf("call 1: expected 1 row, got %d", len(out))
	}
	if out, _ := st.filter(rows); len(out) != 0 {
		t.Fatalf("call 2: expected 0 rows, got %d", len(out))
	}
	if out, _ := st.filter(rows); len(out) != 1 {
		t.Fatalf("call 3 (heartbeat): expected 1 row, got %d", len(out))
	}
}

func TestPgIndexState_DroppedIndexForgotten(t *testing.T) {
	st := newPgIndexState(1000)
	if _, err := st.filter([]PgIndexRow{mkRow("public", "a", 1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.filter([]PgIndexRow{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.hashes[pgIndexKey{Schema: "public", Name: "a"}]; ok {
		t.Fatal("dropped index should be forgotten from cache")
	}
	out, err := st.filter([]PgIndexRow{mkRow("public", "a", 1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("re-appeared index should be emitted, got %d rows", len(out))
	}
}

func TestPgIndexState_RowsMissingKeysDropped(t *testing.T) {
	st := newPgIndexState(1000)
	rp := Integer(1)
	rows := []PgIndexRow{
		{RelPages: &rp},
		mkRow("public", "a", 1),
	}
	out, err := st.filter(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || string(*out[0].IndexName) != "a" {
		t.Fatalf("expected only 'a' in output, got %#v", out)
	}
}
