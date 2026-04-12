package collectorconfig

import (
	"fmt"
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
