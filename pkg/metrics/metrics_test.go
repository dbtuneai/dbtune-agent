package metrics

import (
	"testing"
)

func TestTruncateFloat(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected float64
	}{
		{
			name:     "Already 3 decimals",
			input:    1.234,
			expected: 1.234,
		},
		{
			name:     "More than 3 decimals truncated",
			input:    1.23456789,
			expected: 1.235,
		},
		{
			name:     "Less than 3 decimals unchanged",
			input:    1.5,
			expected: 1.5,
		},
		{
			name:     "Zero",
			input:    0.0,
			expected: 0.0,
		},
		{
			name:     "Negative value",
			input:    -1.23456789,
			expected: -1.235,
		},
		{
			name:     "Very small value",
			input:    0.0001234,
			expected: 0.0,
		},
		{
			name:     "Rounds up correctly",
			input:    1.9999,
			expected: 2.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateFloat(tt.input)
			if result != tt.expected {
				t.Errorf("truncateFloat(%v) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFormatMetrics_TruncatesFloats(t *testing.T) {
	metrics := []FlatValue{
		{Key: "float_metric", Value: 1.23456789, Type: Float},
		{Key: "int_metric", Value: int64(42), Type: Int},
		{Key: "string_metric", Value: "test", Type: String},
	}

	result := FormatMetrics(metrics)

	// Check float is truncated
	floatVal, ok := result.Metrics["float_metric"].(float64)
	if !ok {
		t.Fatal("float_metric should be float64")
	}
	if floatVal != 1.235 {
		t.Errorf("float_metric = %v, expected 1.235", floatVal)
	}

	// Check int is unchanged
	intVal, ok := result.Metrics["int_metric"].(int64)
	if !ok {
		t.Fatal("int_metric should be int64")
	}
	if intVal != 42 {
		t.Errorf("int_metric = %v, expected 42", intVal)
	}

	// Check string is unchanged
	strVal, ok := result.Metrics["string_metric"].(string)
	if !ok {
		t.Fatal("string_metric should be string")
	}
	if strVal != "test" {
		t.Errorf("string_metric = %v, expected test", strVal)
	}
}

func TestHashJSON(t *testing.T) {
	t.Run("deterministic for same input", func(t *testing.T) {
		input := map[string]MetricData{
			"cpu": {Type: "float", Value: 0.5},
			"mem": {Type: "int", Value: 1024},
		}
		hash1, err1 := HashJSON(input)
		hash2, err2 := HashJSON(input)
		if err1 != nil || err2 != nil {
			t.Fatalf("unexpected error: %v, %v", err1, err2)
		}
		if hash1 != hash2 {
			t.Errorf("same input produced different hashes: %s vs %s", hash1, hash2)
		}
	})

	t.Run("different input produces different hash", func(t *testing.T) {
		hash1, _ := HashJSON(map[string]int{"a": 1})
		hash2, _ := HashJSON(map[string]int{"a": 2})
		if hash1 == hash2 {
			t.Error("different inputs produced the same hash")
		}
	})

	t.Run("returns error on unmarshalable input", func(t *testing.T) {
		ch := make(chan int)
		_, err := HashJSON(ch)
		if err == nil {
			t.Error("expected error for unmarshalable input")
		}
	})
}

func TestFormatSystemInfo_TruncatesFloats(t *testing.T) {
	metrics := []FlatValue{
		{Key: "float_metric", Value: 99.99999, Type: Float},
		{Key: "percentage_metric", Value: 50.123456, Type: Percentage},
		{Key: "int_metric", Value: int64(100), Type: Int},
	}

	result := FormatSystemInfo(metrics)

	// Check float is truncated
	floatData := result.SystemInfo["float_metric"]
	floatVal, ok := floatData.Value.(float64)
	if !ok {
		t.Fatal("float_metric value should be float64")
	}
	if floatVal != 100.0 {
		t.Errorf("float_metric = %v, expected 100.0", floatVal)
	}

	// Check percentage (also float64) is truncated
	pctData := result.SystemInfo["percentage_metric"]
	pctVal, ok := pctData.Value.(float64)
	if !ok {
		t.Fatal("percentage_metric value should be float64")
	}
	if pctVal != 50.123 {
		t.Errorf("percentage_metric = %v, expected 50.123", pctVal)
	}

	// Check int is unchanged
	intData := result.SystemInfo["int_metric"]
	intVal, ok := intData.Value.(int64)
	if !ok {
		t.Fatal("int_metric value should be int64")
	}
	if intVal != 100 {
		t.Errorf("int_metric = %v, expected 100", intVal)
	}

	// Check hash is present and deterministic
	if result.Hash == "" {
		t.Error("expected non-empty hash")
	}
	result2 := FormatSystemInfo(metrics)
	if result.Hash != result2.Hash {
		t.Errorf("same input produced different hashes: %s vs %s", result.Hash, result2.Hash)
	}
}
