package collectorconfig

import (
	"fmt"
	"iter"
	"reflect"
	"strconv"
)

// ParseBoolValue converts a raw YAML/env value to bool.
// Accepts bool and string ("true"/"false") types.
func ParseBoolValue(raw any) (bool, error) {
	switch value := raw.(type) {
	case bool:
		return value, nil
	case string:
		return strconv.ParseBool(value)
	default:
		return false, fmt.Errorf("expected boolean, got %T", raw)
	}
}

// ParseIntValue converts a raw YAML/env value to int.
// Accepts int, int64, float64 (no decimals), and string types.
func ParseIntValue(raw any) (int, error) {
	switch value := raw.(type) {
	case int:
		return value, nil
	case int64:
		return int(value), nil
	case float64:
		if value != float64(int(value)) {
			return 0, fmt.Errorf("expected integer, got %v", value)
		}
		return int(value), nil
	case string:
		return strconv.Atoi(value)
	default:
		return 0, fmt.Errorf("expected integer, got %T", raw)
	}
}

// fieldSpec describes a single config struct field, extracted from struct tags.
type fieldSpec struct {
	index       int          // struct field index
	key         string       // config tag value
	valueKind   reflect.Kind // bool or int (unwrapped from pointer)
	isPointer   bool         // field is *bool or *int
	defaultInt  int          // default for non-pointer int fields
	defaultBool bool         // default for non-pointer bool fields
	hasDefault  bool         // whether a default should be applied
	minInt      *int         // min constraint for int fields (nil = none)
	maxInt      *int         // max constraint for int fields (nil = none)
}

// HasDefault reports whether this spec has a default to apply.
func (s *fieldSpec) HasDefault() bool {
	return !s.isPointer && s.hasDefault
}

// SetDefault applies the default value to the given reflect field.
func (s *fieldSpec) SetDefault(field reflect.Value) {
	switch s.valueKind { //nolint:exhaustive // bool/int are the only supported kinds
	case reflect.Bool:
		field.SetBool(s.defaultBool)
	case reflect.Int:
		field.SetInt(int64(s.defaultInt))
	}
}

// SetValue parses raw, validates constraints, and sets the field.
func (s *fieldSpec) SetValue(field reflect.Value, raw any) error {
	switch s.valueKind {
	case reflect.Bool:
		b, err := ParseBoolValue(raw)
		if err != nil {
			return fmt.Errorf("%s: %w", s.key, err)
		}
		if s.isPointer {
			field.Set(reflect.ValueOf(&b))
		} else {
			field.SetBool(b)
		}
	case reflect.Int:
		n, err := ParseIntValue(raw)
		if err != nil {
			return fmt.Errorf("%s: %w", s.key, err)
		}
		if s.minInt != nil && n < *s.minInt {
			return fmt.Errorf("%s must be >= %d", s.key, *s.minInt)
		}
		if s.maxInt != nil && n > *s.maxInt {
			return fmt.Errorf("%s must be <= %d", s.key, *s.maxInt)
		}
		if s.isPointer {
			field.Set(reflect.ValueOf(&n))
		} else {
			field.SetInt(int64(n))
		}
	default:
		return fmt.Errorf("unsupported field type %s for %q", s.valueKind, s.key)
	}
	return nil
}

// fieldSpecs holds all specs for a config struct, plus an allowed-key set.
type fieldSpecs struct {
	all     []fieldSpec
	allowed map[string]struct{}
}

// WithDefaults iterates over specs that have a non-zero default.
func (s *fieldSpecs) WithDefaults() iter.Seq[*fieldSpec] {
	return func(yield func(*fieldSpec) bool) {
		for i := range s.all {
			if s.all[i].HasDefault() {
				if !yield(&s.all[i]) {
					return
				}
			}
		}
	}
}

// buildFieldSpecs reflects over T once and returns specs with all metadata.
func buildFieldSpecs(t reflect.Type) fieldSpecs {
	var all []fieldSpec
	allowed := make(map[string]struct{})
	for i := range t.NumField() {
		f := t.Field(i)
		key := f.Tag.Get("config")
		if key == "" {
			continue
		}
		kind := f.Type.Kind()
		isPtr := kind == reflect.Ptr
		valueKind := kind
		if isPtr {
			valueKind = f.Type.Elem().Kind()
		}
		spec := fieldSpec{index: i, key: key, valueKind: valueKind, isPointer: isPtr}
		if !isPtr {
			if def := f.Tag.Get("default"); def != "" {
				switch valueKind { //nolint:exhaustive // bool/int are the only supported kinds
				case reflect.Int:
					if n, err := strconv.Atoi(def); err == nil {
						spec.defaultInt = n
						spec.hasDefault = true
					}
				case reflect.Bool:
					if b, err := strconv.ParseBool(def); err == nil {
						spec.defaultBool = b
						spec.hasDefault = true
					}
				}
			}
		}
		if valueKind == reflect.Int {
			if minStr := f.Tag.Get("min"); minStr != "" {
				n, _ := strconv.Atoi(minStr)
				spec.minInt = &n
			}
			if maxStr := f.Tag.Get("max"); maxStr != "" {
				n, _ := strconv.Atoi(maxStr)
				spec.maxInt = &n
			}
		}
		all = append(all, spec)
		allowed[key] = struct{}{}
	}
	return fieldSpecs{all: all, allowed: allowed}
}

// parse is the shared implementation for Parse and ParsePartial.
func parse[T any](raw map[string]any, rejectUnknown bool) (T, map[string]any, error) {
	var zero T
	t := reflect.TypeOf(zero)
	specs := buildFieldSpecs(t)

	// Separate known vs unknown keys.
	var remaining map[string]any
	for k := range raw {
		if _, ok := specs.allowed[k]; !ok {
			if rejectUnknown {
				return zero, nil, fmt.Errorf("unknown field %q", k)
			}
			if remaining == nil {
				remaining = make(map[string]any)
			}
			remaining[k] = raw[k]
		}
	}

	// Start with defaults (pointer fields stay nil).
	result := reflect.New(t).Elem()
	for spec := range specs.WithDefaults() {
		spec.SetDefault(result.Field(spec.index))
	}

	// Apply raw values.
	for i := range specs.all {
		spec := &specs.all[i]
		v, ok := raw[spec.key]
		if !ok {
			continue
		}
		if err := spec.SetValue(result.Field(spec.index), v); err != nil {
			return zero, nil, err
		}
	}

	return result.Interface().(T), remaining, nil
}

// Parse parses a raw field map into a config struct T using struct tags.
// Unknown keys are rejected. See ParsePartial for a variant that passes
// unknown keys through.
//
// Supported struct tags:
//   - `config:"key_name"` — the YAML/env field name (required for the field to be parsed)
//   - `default:"123"` — default value for non-pointer int fields (bool defaults to false, int defaults to 0)
//   - `min:"0"` — minimum value for int fields (inclusive)
//   - `max:"500"` — maximum value for int fields (inclusive)
//
// Pointer fields (*bool, *int) default to nil (unset).
func Parse[T any](raw map[string]any) (T, error) {
	result, _, err := parse[T](raw, true)
	return result, err
}

// ParsePartial is like Parse but passes unknown keys through in the
// returned remaining map instead of rejecting them.
func ParsePartial[T any](raw map[string]any) (T, map[string]any, error) {
	return parse[T](raw, false)
}
