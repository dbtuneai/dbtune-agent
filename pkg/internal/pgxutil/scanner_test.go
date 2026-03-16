package pgxutil

import "testing"

func TestNewScanner_PanicsOnNonStruct(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for non-struct type")
		}
	}()
	NewScanner[int]()
}
