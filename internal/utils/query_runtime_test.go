package utils

import (
	"testing"
)

// TestCalculateQueryRuntime tests the CalculateQueryRuntime function.
func TestCalculateQueryRuntime(t *testing.T) {
	tests := []struct {
		name     string
		prev     map[string]CachedPGStatStatement
		curr     map[string]CachedPGStatStatement
		expected float64
	}{
		{
			name: "Basic increase in calls and exec time",
			prev: map[string]CachedPGStatStatement{
				"query1": {QueryId: "query1", Calls: 10, TotalExecTime: 5.0},
				"query2": {QueryId: "query2", Calls: 10, TotalExecTime: 5.0},
			},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryId: "query1", Calls: 20, TotalExecTime: 10.0},
				"query2": {QueryId: "query2", Calls: 20, TotalExecTime: 10.0},
			},
			expected: 0.5, // (5 + 5) / (10 + 10) = 10 / 20 = 0.5 ms
		},
		{
			name: "No increase in calls or exec time",
			prev: map[string]CachedPGStatStatement{
				"query1": {QueryId: "query1", Calls: 10, TotalExecTime: 100.0},
			},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryId: "query1", Calls: 10, TotalExecTime: 100.0},
			},
			expected: 0.0, // No calls or exec time increase
		},
		{
			name: "New query in current snapshot",
			prev: map[string]CachedPGStatStatement{
				"query1": {QueryId: "query1", Calls: 10, TotalExecTime: 100.0},
			},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryId: "query1", Calls: 10, TotalExecTime: 100.0},
				"query2": {QueryId: "query2", Calls: 5, TotalExecTime: 50.0},
			},
			// ((100 - 100) + (50 - 0)) / ((10 - 10) + (5 - 0)) = 50 / 5 = 10 ms
			expected: 10.0,
		},
		{
			name: "Query removed in current snapshot",
			prev: map[string]CachedPGStatStatement{
				"query1": {QueryId: "query1", Calls: 10, TotalExecTime: 100.0},
				"query2": {QueryId: "query2", Calls: 5, TotalExecTime: 50.0},
			},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryId: "query1", Calls: 15, TotalExecTime: 150.0},
			},
			expected: 10.0, // Only query1 is considered
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateQueryRuntime(tt.prev, tt.curr)
			if result != tt.expected {
				t.Errorf("expected %.2f, got %.2f", tt.expected, result)
			}
		})
	}
}
