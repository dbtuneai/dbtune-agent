package pgxutil

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const structTagKey = "db"

// structField holds pre-computed info for a single exported struct field.
type structField struct {
	name  string // db-facing name (from tag or lowercased field name)
	index []int  // reflect index path (supports embedded structs)
}

// columnMapping is the cached result of matching a specific column layout
// to a struct's fields. Built once per unique column set, reused for every
// subsequent row with the same layout.
type columnMapping struct {
	// For each column position i in the result set:
	//   fieldIndex[i] >= 0  → index into Scanner.fields for the matching struct field
	//   fieldIndex[i] == -1 → no matching struct field, skip this column
	fieldIndex []int
}

// Scanner pre-computes struct reflection at construction time and caches
// column-to-field mappings keyed by the actual column layout returned by
// PostgreSQL. Different PG versions returning different columns from SELECT *
// each get their own cached mapping. Scanner is safe for concurrent use.
//
// Usage:
//
//	var pgStatScanner = pgxutil.NewScanner[PgStatDatabase]()
//
//	rows, err := pool.Query(ctx, "SELECT * FROM pg_stat_database")
//	results, err := pgx.CollectRows(rows, pgStatScanner.Scan)
type Scanner[T any] struct {
	fields []structField  // pre-computed at NewScanner time
	byName map[string]int // field name → index into fields, pre-computed
	cache  sync.Map       // colKey (string) → *columnMapping
	typ    reflect.Type   // cached reflect.Type of T
}

// NewScanner creates a Scanner for struct type T. Call this once at
// package init or startup — it does all struct reflection up front.
//
// Panics if T is not a struct.
func NewScanner[T any]() *Scanner[T] {
	var zero T
	t := reflect.TypeOf(zero)
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("pgxutil.NewScanner: T must be a struct, got %s", t.Kind()))
	}

	s := &Scanner[T]{typ: t}
	s.fields = computeStructFields(t)

	s.byName = make(map[string]int, len(s.fields))
	for i, f := range s.fields {
		s.byName[f.name] = i
	}

	return s
}

// Scan implements pgx.RowToFunc[T]. Row columns with no matching struct
// field are skipped; struct fields with no matching column get zero values.
func (s *Scanner[T]) Scan(row pgx.CollectableRow) (T, error) {
	var result T

	fldDescs := row.FieldDescriptions()
	mapping := s.getMapping(fldDescs)

	scanTargets := make([]any, len(fldDescs))
	rv := reflect.ValueOf(&result).Elem()

	for colIdx, fieldIdx := range mapping.fieldIndex {
		if fieldIdx < 0 {
			// No struct field for this column — nil tells Scan to skip it.
			scanTargets[colIdx] = nil
			continue
		}
		// Navigate the index path to reach the (possibly embedded) field.
		field := rv
		for _, idx := range s.fields[fieldIdx].index {
			field = field.Field(idx)
		}
		scanTargets[colIdx] = field.Addr().Interface()
	}

	if err := row.Scan(scanTargets...); err != nil {
		return result, err
	}
	return result, nil
}

// ScanAddr is like Scan but returns a pointer, matching the pgx RowToAddrOf* convention.
func (s *Scanner[T]) ScanAddr(row pgx.CollectableRow) (*T, error) {
	result, err := s.Scan(row)
	return &result, err
}

// getMapping returns the columnMapping for the given field descriptions,
// using a cached value if available.
func (s *Scanner[T]) getMapping(fldDescs []pgconn.FieldDescription) *columnMapping {
	key := colKey(fldDescs)

	if cached, ok := s.cache.Load(key); ok {
		return cached.(*columnMapping)
	}

	mapping := s.buildMapping(fldDescs)

	// Store-or-load: if another goroutine raced us, use theirs (identical).
	actual, _ := s.cache.LoadOrStore(key, mapping)
	return actual.(*columnMapping)
}

// buildMapping creates a columnMapping by matching each column name to a
// struct field. O(columns) with the pre-built name map.
func (s *Scanner[T]) buildMapping(fldDescs []pgconn.FieldDescription) *columnMapping {
	m := &columnMapping{
		fieldIndex: make([]int, len(fldDescs)),
	}
	for i, fd := range fldDescs {
		colName := strings.ToLower(fd.Name)
		if idx, ok := s.byName[colName]; ok {
			m.fieldIndex[i] = idx
		} else {
			m.fieldIndex[i] = -1
		}
	}
	return m
}

// colKey builds a cache key from the column names.
func colKey(fldDescs []pgconn.FieldDescription) string {
	var b strings.Builder
	b.Grow(len(fldDescs) * 16)
	for i, fd := range fldDescs {
		if i > 0 {
			b.WriteByte(0) // null byte separator — can't appear in column names
		}
		b.WriteString(fd.Name)
	}
	return b.String()
}

// computeStructFields walks a struct type and collects all exported,
// taggable fields, recursing into anonymous (embedded) structs.
func computeStructFields(t reflect.Type) []structField {
	var fields []structField
	var walk func(t reflect.Type, indexPath []int)
	walk = func(t reflect.Type, indexPath []int) {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			currentPath := make([]int, len(indexPath)+1)
			copy(currentPath, indexPath)
			currentPath[len(indexPath)] = i

			if !f.IsExported() {
				continue
			}

			if f.Anonymous && f.Type.Kind() == reflect.Struct {
				walk(f.Type, currentPath)
				continue
			}

			tag := f.Tag.Get(structTagKey)
			if tag == "-" {
				continue
			}

			name := tag
			if name == "" {
				name = strings.ToLower(f.Name)
			}

			fields = append(fields, structField{
				name:  name,
				index: currentPath,
			})
		}
	}
	walk(t, nil)
	return fields
}
