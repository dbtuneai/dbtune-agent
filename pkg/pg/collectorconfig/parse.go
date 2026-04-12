package collectorconfig

import (
	"fmt"
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
	index      int          // struct field index
	key        string       // config tag value
	valueKind  reflect.Kind // bool or int (unwrapped from pointer)
	isPointer  bool         // field is *bool or *int
	defaultInt int          // default for non-pointer int fields
}

// buildFieldSpecs reflects over T once and returns the field specs + a map of allowed keys.
func buildFieldSpecs(t reflect.Type) ([]fieldSpec, map[string]struct{}) {
	var specs []fieldSpec
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
			if def := f.Tag.Get("default"); def != "" && valueKind == reflect.Int {
				n, _ := strconv.Atoi(def)
				spec.defaultInt = n
			}
		}
		specs = append(specs, spec)
		allowed[key] = struct{}{}
	}
	return specs, allowed
}

// parse is the shared implementation for Parse and ParsePartial.
func parse[T any](raw map[string]any, rejectUnknown bool) (T, map[string]any, error) {
	var zero T
	t := reflect.TypeOf(zero)
	specs, allowed := buildFieldSpecs(t)

	// Separate known vs unknown keys.
	var remaining map[string]any
	for k := range raw {
		if _, ok := allowed[k]; !ok {
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
	for _, spec := range specs {
		if !spec.isPointer && spec.valueKind == reflect.Int && spec.defaultInt != 0 {
			result.Field(spec.index).SetInt(int64(spec.defaultInt))
		}
	}

	// Apply raw values.
	for _, spec := range specs {
		v, ok := raw[spec.key]
		if !ok {
			continue
		}
		field := result.Field(spec.index)
		structField := t.Field(spec.index)
		switch spec.valueKind {
		case reflect.Bool:
			b, err := ParseBoolValue(v)
			if err != nil {
				return zero, nil, fmt.Errorf("%s: %w", spec.key, err)
			}
			if spec.isPointer {
				field.Set(reflect.ValueOf(&b))
			} else {
				field.SetBool(b)
			}
		case reflect.Int:
			n, err := ParseIntValue(v)
			if err != nil {
				return zero, nil, fmt.Errorf("%s: %w", spec.key, err)
			}
			if minStr := structField.Tag.Get("min"); minStr != "" {
				minVal, _ := strconv.Atoi(minStr)
				if n < minVal {
					return zero, nil, fmt.Errorf("%s must be >= %d", spec.key, minVal)
				}
			}
			if maxStr := structField.Tag.Get("max"); maxStr != "" {
				maxVal, _ := strconv.Atoi(maxStr)
				if n > maxVal {
					return zero, nil, fmt.Errorf("%s must be <= %d", spec.key, maxVal)
				}
			}
			if spec.isPointer {
				field.Set(reflect.ValueOf(&n))
			} else {
				field.SetInt(int64(n))
			}
		default:
			return zero, nil, fmt.Errorf("unsupported field type %s for %q", spec.valueKind, spec.key)
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
