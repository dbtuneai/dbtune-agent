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
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return false, err
		}
		return parsed, nil
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
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("expected integer, got %T", raw)
	}
}

// fieldSpec describes a single config struct field, extracted from struct tags.
type fieldSpec struct {
	index      int // struct field index
	key        string
	kind       reflect.Kind // bool or int
	defaultInt int
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
		spec := fieldSpec{index: i, key: key, kind: f.Type.Kind()}
		if def := f.Tag.Get("default"); def != "" && spec.kind == reflect.Int {
			n, _ := strconv.Atoi(def)
			spec.defaultInt = n
		}
		specs = append(specs, spec)
		allowed[key] = struct{}{}
	}
	return specs, allowed
}

// Parse parses a raw field map into a config struct T using struct tags.
//
// Supported struct tags:
//   - `config:"key_name"` — the YAML/env field name (required for the field to be parsed)
//   - `default:"123"` — default value for int fields (bool defaults to false, int defaults to 0)
//   - `min:"0"` — minimum value for int fields (inclusive)
//   - `max:"500"` — maximum value for int fields (inclusive)
func Parse[T any](raw map[string]any) (T, error) {
	var zero T
	t := reflect.TypeOf(zero)
	specs, allowed := buildFieldSpecs(t)

	// Reject unknown keys.
	for k := range raw {
		if _, ok := allowed[k]; !ok {
			return zero, fmt.Errorf("unknown field %q", k)
		}
	}

	// Start with defaults.
	result := reflect.New(t).Elem()
	for _, spec := range specs {
		if spec.kind == reflect.Int && spec.defaultInt != 0 {
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
		switch spec.kind {
		case reflect.Bool:
			b, err := ParseBoolValue(v)
			if err != nil {
				return zero, fmt.Errorf("%s: %w", spec.key, err)
			}
			field.SetBool(b)
		case reflect.Int:
			n, err := ParseIntValue(v)
			if err != nil {
				return zero, fmt.Errorf("%s: %w", spec.key, err)
			}
			if minStr := structField.Tag.Get("min"); minStr != "" {
				minVal, _ := strconv.Atoi(minStr)
				if n < minVal {
					return zero, fmt.Errorf("%s must be >= %d", spec.key, minVal)
				}
			}
			if maxStr := structField.Tag.Get("max"); maxStr != "" {
				maxVal, _ := strconv.Atoi(maxStr)
				if n > maxVal {
					return zero, fmt.Errorf("%s must be <= %d", spec.key, maxVal)
				}
			}
			field.SetInt(int64(n))
		default:
			return zero, fmt.Errorf("unsupported field type %s for %q", spec.kind, spec.key)
		}
	}

	return result.Interface().(T), nil
}
