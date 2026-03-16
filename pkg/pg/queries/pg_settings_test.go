package queries

import (
	"testing"
)

func TestInferNumericType(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{"integer string", "42", int64(42)},
		{"float string", "3.14", float64(3.14)},
		{"non-numeric string", "on", "on"},
		{"non-string passthrough", int64(99), int64(99)},
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InferNumericType(tt.input)
			if got != tt.want {
				t.Errorf("InferNumericType(%v) = %v (%T), want %v (%T)",
					tt.input, got, got, tt.want, tt.want)
			}
		})
	}
}
