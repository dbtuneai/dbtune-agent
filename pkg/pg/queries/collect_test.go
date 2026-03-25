package queries

import (
	"testing"
)

func TestSkipTracker_FirstCallNeverSkips(t *testing.T) {
	tr := newSkipTracker(30)
	if tr.shouldSkip([]byte(`{"rows":[]}`)) {
		t.Fatal("first call should never skip")
	}
}

func TestSkipTracker_IdenticalDataSkips(t *testing.T) {
	tr := newSkipTracker(30)
	data := []byte(`{"rows":[{"a":1}]}`)

	tr.shouldSkip(data) // first call, initialises
	if !tr.shouldSkip(data) {
		t.Fatal("identical data on second call should skip")
	}
}

func TestSkipTracker_DifferentDataDoesNotSkip(t *testing.T) {
	tr := newSkipTracker(30)

	tr.shouldSkip([]byte(`{"rows":[{"a":1}]}`))
	if tr.shouldSkip([]byte(`{"rows":[{"a":2}]}`)) {
		t.Fatal("different data should not skip")
	}
}

func TestSkipTracker_ForceAfterThreshold(t *testing.T) {
	const forceAfter = 5
	tr := newSkipTracker(forceAfter)
	data := []byte(`{"rows":[]}`)

	tr.shouldSkip(data) // call 0: first, never skips

	// calls 1..forceAfter should skip (unchanged)
	for i := 1; i <= forceAfter-1; i++ {
		if !tr.shouldSkip(data) {
			t.Fatalf("call %d: should skip (unchanged, under threshold)", i)
		}
	}

	// call at forceAfter should NOT skip (force send)
	if tr.shouldSkip(data) {
		t.Fatalf("call %d: should force-send at threshold", forceAfter)
	}
}

func TestSkipTracker_ForceRepeats(t *testing.T) {
	const forceAfter = 3
	tr := newSkipTracker(forceAfter)
	data := []byte(`same`)

	tr.shouldSkip(data) // init

	// Run through two full force cycles to confirm it repeats
	for cycle := 0; cycle < 2; cycle++ {
		for i := 0; i < forceAfter-1; i++ {
			if !tr.shouldSkip(data) {
				t.Fatalf("cycle %d, call %d: should skip", cycle, i)
			}
		}
		if tr.shouldSkip(data) {
			t.Fatalf("cycle %d: should force-send", cycle)
		}
	}
}

func TestSkipTracker_ChangeResetsCounter(t *testing.T) {
	const forceAfter = 10
	tr := newSkipTracker(forceAfter)
	dataA := []byte(`a`)
	dataB := []byte(`b`)

	tr.shouldSkip(dataA) // init with A

	// Skip a few times with same data
	for i := 0; i < 5; i++ {
		tr.shouldSkip(dataA)
	}

	// Change data — should not skip and should reset counter
	if tr.shouldSkip(dataB) {
		t.Fatal("changed data should not skip")
	}

	// Now unchanged count is reset; should skip again for forceAfter-1 calls
	for i := 0; i < forceAfter-1; i++ {
		if !tr.shouldSkip(dataB) {
			t.Fatalf("call %d after change: should skip", i)
		}
	}

	// Force send
	if tr.shouldSkip(dataB) {
		t.Fatal("should force-send after threshold")
	}
}

func TestCollectResult_Hash_Deterministic(t *testing.T) {
	r := &CollectResult{JSON: []byte(`{"rows":[{"a":1}]}`)}
	h1 := r.Hash()
	h2 := r.Hash()
	if h1 != h2 {
		t.Fatalf("hash should be deterministic: %q != %q", h1, h2)
	}
	if h1 == "" {
		t.Fatal("hash should not be empty")
	}
}

func TestCollectResult_Hash_DifferentData(t *testing.T) {
	r1 := &CollectResult{JSON: []byte(`{"rows":[{"a":1}]}`)}
	r2 := &CollectResult{JSON: []byte(`{"rows":[{"a":2}]}`)}
	if r1.Hash() == r2.Hash() {
		t.Fatal("different data should produce different hashes")
	}
}
