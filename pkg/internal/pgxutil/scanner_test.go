package pgxutil

import (
	"slices"
	"testing"
)

func TestNewScanner_PanicsOnNonStruct(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for non-struct type")
		}
	}()
	NewScanner[int]()
}

func TestNewScanner_EmbeddedStruct(t *testing.T) {
	type Embedded struct {
		EmbeddedField       int
		SecondEmbeddedField int
	}

	type example struct {
		Field1 int
		Embedded
		Field2 string
	}

	expectedFields := map[string][]int{
		"field1":              []int{0},
		"embeddedfield":       []int{1, 0},
		"secondembeddedfield": []int{1, 1},
		"field2":              []int{2},
	}

	s := NewScanner[example]()

	if len(expectedFields) != len(s.indexPathByName) {
		t.Errorf("Expected %d fields, got %d", len(expectedFields), len(s.indexPathByName))
	}

	for key, value := range s.indexPathByName {
		if expected, ok := expectedFields[key]; ok {
			pathsEqual := slices.Equal(value, expected)
			if !pathsEqual {
				t.Errorf("Unexpected path for field %s: got %v, want %v", key, value, expected)
			}
		} else {
			t.Errorf("Unexpected field in results with key %s: %v", key, value)
		}
	}
}
