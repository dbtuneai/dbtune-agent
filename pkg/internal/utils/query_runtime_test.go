package utils

import (
	"slices"
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
				"query1": {QueryID: "query1", Calls: 10, TotalExecTime: 5.0},
				"query2": {QueryID: "query2", Calls: 10, TotalExecTime: 5.0},
			},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 20, TotalExecTime: 10.0},
				"query2": {QueryID: "query2", Calls: 20, TotalExecTime: 10.0},
			},
			expected: 0.5, // (5 + 5) / (10 + 10) = 10 / 20 = 0.5 ms
		},
		{
			name: "No increase in calls or exec time",
			prev: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 10, TotalExecTime: 100.0},
			},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 10, TotalExecTime: 100.0},
			},
			expected: 0.0, // No calls or exec time increase
		},
		{
			name: "New query in current snapshot",
			prev: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 10, TotalExecTime: 100.0},
			},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 10, TotalExecTime: 100.0},
				"query2": {QueryID: "query2", Calls: 5, TotalExecTime: 50.0},
			},
			// ((100 - 100) + (50 - 0)) / ((10 - 10) + (5 - 0)) = 50 / 5 = 10 ms
			expected: 10.0,
		},
		{
			name: "Query removed in current snapshot",
			prev: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 10, TotalExecTime: 100.0},
				"query2": {QueryID: "query2", Calls: 5, TotalExecTime: 50.0},
			},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 15, TotalExecTime: 150.0},
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

func TestCalculateQueryRuntimeDelta(t *testing.T) {
	tests := []struct {
		name        string
		prev        map[string]CachedPGStatStatement
		curr        map[string]CachedPGStatStatement
		expected    []CachedPGStatStatement
		expectedNum int
	}{
		{
			name: "Basic increase in calls and exec time",
			prev: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 10, TotalExecTime: 5.0},
			},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 20, TotalExecTime: 10.0},
			},
			expected: []CachedPGStatStatement{
				{QueryID: "query1", Calls: 10, TotalExecTime: 5.0},
			},
			expectedNum: 1,
		},
		{
			name: "Decrease in calls and exec time",
			prev: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 10, TotalExecTime: 5.0},
			},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 5, TotalExecTime: 4.0},
			},
			// Should not include any diff
			expected:    []CachedPGStatStatement{},
			expectedNum: 0,
		},
		{
			name: "Entry existing only in current but not in previous",
			prev: map[string]CachedPGStatStatement{},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 10, TotalExecTime: 5.0},
			},
			expected: []CachedPGStatStatement{
				{QueryID: "query1", Calls: 10, TotalExecTime: 5.0},
			},
			expectedNum: 1,
		},
		{
			name: "Multiple entries with custom limit of 3",
			prev: map[string]CachedPGStatStatement{},
			curr: map[string]CachedPGStatStatement{
				"query1": {QueryID: "query1", Calls: 10, TotalExecTime: 50.0},
				"query2": {QueryID: "query2", Calls: 20, TotalExecTime: 200.0},
				"query3": {QueryID: "query3", Calls: 5, TotalExecTime: 100.0},
				"query4": {QueryID: "query4", Calls: 15, TotalExecTime: 45.0},
				"query5": {QueryID: "query5", Calls: 25, TotalExecTime: 50.0},
			},
			expected: []CachedPGStatStatement{
				{QueryID: "query3", Calls: 5, TotalExecTime: 100.0},
				{QueryID: "query2", Calls: 20, TotalExecTime: 200.0},
				{QueryID: "query1", Calls: 10, TotalExecTime: 50.0},
			},
			expectedNum: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diffLimit = 3
			diff, totalDiffs := CalculateQueryRuntimeDelta(tt.prev, tt.curr)

			// Check total diffs
			if totalDiffs != tt.expectedNum {
				t.Errorf("expected %d total diffs, got %d", tt.expectedNum, totalDiffs)
			}

			// Check slice equality using slices.Equal with a custom equal function
			equal := slices.EqualFunc(diff, tt.expected, func(a, b CachedPGStatStatement) bool {
				return a.QueryID == b.QueryID &&
					a.Calls == b.Calls &&
					a.TotalExecTime == b.TotalExecTime
			})

			if !equal {
				t.Errorf("slices are not equal\nexpected: %+v\ngot: %+v", tt.expected, diff)
			}
		})
	}
}
