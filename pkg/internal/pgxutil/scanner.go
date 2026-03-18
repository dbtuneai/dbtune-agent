package pgxutil

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
)

const structTagKey = "db"

// structField holds pre-computed info for a single exported struct field.
type structField struct {
	name  string // db-facing name (from tag or lowercased field name)
	index []int  // reflect index path (supports embedded structs)
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
	// Map from field name to index path. This is written at the point
	// of struct instantiation by NewScanner. Later concurrent reads from
	// this map are fine, but if any other way of writing is added then
	// that will require synchronisation.
	indexPathByName map[string][]int
}

// NewScanner creates a Scanner for struct type T. Call this once at
// package init or startup. It holds a map from field name (as expected
// from the database) to the index path of the struct. This "cuts" out
// the index lookup that would be required at runtime if instead it
// relied on `reflect.Value.FieldByName`.
//
// Panics if T is not a struct.
func NewScanner[T any]() *Scanner[T] {
	var zero T
	t := reflect.TypeOf(zero)
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("pgxutil.NewScanner: T must be a struct, got %s", t.Kind()))
	}

	s := &Scanner[T]{}
	fields := computeStructFields(t)

	s.indexPathByName = make(map[string][]int, len(fields))
	for _, f := range fields {
		s.indexPathByName[f.name] = f.index
	}

	return s
}

// Scan implements pgx.RowToFunc[T]. Row columns with no matching struct
// field are skipped; struct fields with no matching column get zero values.
func (s *Scanner[T]) Scan(row pgx.CollectableRow) (T, error) {
	var result T

	fldDescs := row.FieldDescriptions()

	scanTargets := make([]any, len(fldDescs))
	rv := reflect.ValueOf(&result).Elem()

	for resultIndex, pgField := range fldDescs {
		fieldName := strings.ToLower(pgField.Name)
		structIndexPath, ok := s.indexPathByName[fieldName]
		if !ok {
			continue
		}
		scanTargets[resultIndex] = rv.FieldByIndex(structIndexPath).Addr().Interface()
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

// computeStructFields walks a struct type and collects all exported,
// taggable fields, recursing into anonymous (embedded) structs.
// This function will panic if t is not a struct
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
