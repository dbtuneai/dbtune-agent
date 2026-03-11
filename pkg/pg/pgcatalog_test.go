package pg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePgMajorVersion(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"standard version", "16.2", 16},
		{"major only", "17", 17},
		{"three part version", "14.11.1", 14},
		{"pg 13", "13.4", 13},
		{"pg 18", "18.0", 18},
		{"empty string", "", 0},
		{"non-numeric", "abc.def", 0},
		{"non-numeric major", "abc", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParsePgMajorVersion(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
